use std::io::Read;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;

static NEXT_ID: AtomicU32 = AtomicU32::new(0);

fn test_dir() -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let tmp = std::env::temp_dir();
    tmp.join(format!("conrt-test-daemon.{:x}", id))
}

fn conrt_binary() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("conrt");
    path
}

fn start_daemon(socket: &Path) -> Child {
    let dir = socket.parent().unwrap();
    std::fs::create_dir_all(dir).unwrap();

    let child = Command::new(conrt_binary())
        .args(["daemon", "--socket-path", socket.to_str().unwrap()])
        .spawn()
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if socket.exists() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("daemon did not start within 5s");
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    child
}

fn send_request(socket: &PathBuf, payload: &[u8]) -> Vec<u8> {
    let mut stream = UnixStream::connect(socket).unwrap();

    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).unwrap();
    stream.write_all(payload).unwrap();
    stream.flush().unwrap();

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let resp_len = u32::from_le_bytes(len_buf) as usize;

    let mut resp = vec![0u8; resp_len];
    stream.read_exact(&mut resp).unwrap();

    resp
}

fn stop_daemon(mut daemon: Child) {
    daemon.kill().ok();
    let _ = daemon.wait();
}

fn run_conrt(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(conrt_binary()).args(args).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (output.status.success(), stdout, stderr)
}

#[test]
fn protocol_run_missing_field() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = start_daemon(&socket);

    let resp = send_request(&socket, b"{}");
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(!v["ok"].as_bool().unwrap());
    assert!(v["error"].as_str().unwrap().contains("invalid request"));

    stop_daemon(daemon);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn protocol_unknown_type() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = start_daemon(&socket);

    let resp = send_request(&socket, br#"{"type":"Unknown"}"#);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(!v["ok"].as_bool().unwrap());
    assert!(v["error"].as_str().unwrap().contains("unknown variant"));

    stop_daemon(daemon);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn list_returns_empty_initially() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = start_daemon(&socket);

    let resp = send_request(&socket, br#"{"type":"List"}"#);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(v["containers"].as_array().unwrap().len(), 0);

    stop_daemon(daemon);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn kill_unknown_container_returns_error() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = start_daemon(&socket);

    let resp = send_request(&socket, br#"{"type":"Kill","pid":99999}"#);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(!v["ok"].as_bool().unwrap());
    assert!(v["error"].as_str().unwrap().contains("not found"));

    stop_daemon(daemon);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn run_and_list_and_kill_flow() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = start_daemon(&socket);

    let (run_ok, stdout, stderr) = run_conrt(&[
        "run",
        "--detach",
        "--socket-path",
        socket.to_str().unwrap(),
        "--",
        "/bin/sleep",
        "100",
    ]);
    assert!(run_ok, "detached run should succeed, stderr: {stderr}");
    let pid: i32 = stdout.trim().parse().expect("stdout should be a PID");
    assert!(pid > 0);

    // List should show it
    let resp = send_request(&socket, br#"{"type":"List"}"#);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let containers = v["containers"].as_array().unwrap();
    assert_eq!(containers.len(), 1);
    assert_eq!(containers[0]["pid"], pid);
    assert!(containers[0]["command"].as_str().unwrap().contains("sleep"));

    // Kill it
    let kill_req = format!(r#"{{"type":"Kill","pid":{pid}}}"#);
    let resp = send_request(&socket, kill_req.as_bytes());
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(v["ok"].as_bool().unwrap(), "kill should succeed: {v:?}");

    // Wait for reaping (poll until empty or timeout)
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resp = send_request(&socket, br#"{"type":"List"}"#);
        let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        if v["containers"].as_array().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("container was not reaped within 5s after kill");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    stop_daemon(daemon);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn detach_cli_output_is_pid() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = start_daemon(&socket);

    let (ok, stdout, stderr) = run_conrt(&[
        "run",
        "--detach",
        "--socket-path",
        socket.to_str().unwrap(),
        "--",
        "/bin/sleep",
        "50",
    ]);
    assert!(ok, "detach should succeed, stderr: {stderr}");
    let pid: i32 = stdout.trim().parse().expect("stdout must be a PID");
    assert!(pid > 0, "PID must be positive, got {pid}");

    // Clean up
    let kill_req = format!(r#"{{"type":"Kill","pid":{pid}}}"#);
    send_request(&socket, kill_req.as_bytes());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resp = send_request(&socket, br#"{"type":"List"}"#);
        let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        if v["containers"].as_array().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() > deadline {
            break; // best-effort cleanup
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    stop_daemon(daemon);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn detach_with_tty_rejected() {
    let (ok, _, stderr) = run_conrt(&["run", "--detach", "-t", "--", "/bin/true"]);
    assert!(!ok, "--detach with -t should fail");
    assert!(
        stderr.contains("incompatible"),
        "stderr should mention incompatibility"
    );
}

#[test]
fn cli_list_without_daemon_errors() {
    let (ok, _, stderr) = run_conrt(&["list", "--socket-path", "/tmp/conrt-test-nonexistent.sock"]);
    assert!(!ok, "list without daemon should fail");
    assert!(
        stderr.contains("cannot connect"),
        "stderr should mention connection failure"
    );
}

#[test]
fn logs_returns_container_output() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = start_daemon(&socket);

    let (ok, stdout, stderr) = run_conrt(&[
        "run",
        "--detach",
        "--socket-path",
        socket.to_str().unwrap(),
        "--",
        "/bin/sh",
        "-c",
        "echo hello; echo world",
    ]);
    assert!(ok, "detached run should succeed, stderr: {stderr}");
    let pid: i32 = stdout.trim().parse().expect("stdout should be a PID");

    // Wait for the container to exit
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let resp = send_request(&socket, br#"{"type":"List"}"#);
        let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
        if v["containers"].as_array().unwrap().is_empty() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("container did not exit within 5s");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Fetch logs via CLI
    let (ok, stdout, stderr) = run_conrt(&[
        "logs",
        "--socket-path",
        socket.to_str().unwrap(),
        &pid.to_string(),
    ]);
    assert!(ok, "logs should succeed, stderr: {stderr}");
    assert!(
        stdout.contains("hello"),
        "logs should contain 'hello', got: {stdout:?}"
    );
    assert!(
        stdout.contains("world"),
        "logs should contain 'world', got: {stdout:?}"
    );

    stop_daemon(daemon);
    std::fs::remove_dir_all(&dir).ok();
}
