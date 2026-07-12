use std::io::Read;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

/// Path where we expect the Alpine test rootfs to be.
/// Override with `CONRT_TEST_ROOTFS` environment variable.
fn test_rootfs() -> String {
    std::env::var("CONRT_TEST_ROOTFS").unwrap_or_else(|_| "/tmp/alpine".into())
}

fn conrt_binary() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("conrt");
    path
}

static NEXT_ID: AtomicU32 = AtomicU32::new(0);

fn test_dir() -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let tmp = std::env::temp_dir();
    tmp.join(format!("conrt-test-container.{:x}", id))
}

/// Starts a daemon and kills it on drop.
struct Daemon(Child, PathBuf);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = unsafe { libc::kill(self.0.id() as i32, libc::SIGKILL) };
        let _ = self.0.wait();
        let _ = std::fs::remove_file(self.1.join("conrt.sock.stream"));
        let _ = std::fs::remove_file(self.1.join("conrt.sock"));
    }
}

impl Daemon {
    fn new() -> Self {
        let dir = test_dir();
        std::fs::create_dir_all(&dir).unwrap();
        let socket = dir.join("conrt.sock");

        let mut child = Command::new(conrt_binary())
            .args(["daemon", "--socket-path", socket.to_str().unwrap()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        // Wait for the socket to appear.
        let stream = dir.join("conrt.sock.stream");
        for _ in 0..100 {
            if stream.exists() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        if !stream.exists() {
            let _ = unsafe { libc::kill(child.id() as i32, libc::SIGKILL) };
            let _ = child.wait();
            panic!("daemon did not start in time");
        }

        Self(child, dir)
    }

    fn socket(&self) -> PathBuf {
        self.1.join("conrt.sock")
    }
}

fn run_conrt(daemon: &Daemon, args: &[&str]) -> (bool, String, String) {
    let socket = daemon.socket();
    let socket_str = socket.to_str().unwrap();
    // --socket-path must go after the subcommand (e.g. "run --socket-path ...").
    let mut full_args = Vec::with_capacity(args.len() + 2);
    full_args.push(args[0]); // subcommand
    full_args.push("--socket-path");
    full_args.push(socket_str);
    full_args.extend_from_slice(&args[1..]);
    let output = Command::new(conrt_binary())
        .args(&full_args)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    (output.status.success(), stdout, stderr)
}

/// Strip tracing log lines from stdout, returning only the container's output.
fn container_stdout(stdout: &str) -> String {
    stdout
        .lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            // Tracing lines start with ANSI escape or a timestamp YYYY-MM-DDTHH:MM
            !(trimmed.starts_with('\u{1b}')
                || trimmed.len() > 10
                    && trimmed.as_bytes()[4] == b'-'
                    && trimmed.as_bytes()[7] == b'-'
                    && trimmed.as_bytes()[10] == b'T')
        })
        .collect::<Vec<_>>()
        .join("\n")
}

static ROOTFS: OnceLock<Option<String>> = OnceLock::new();

fn ensure_rootfs() -> Option<String> {
    ROOTFS
        .get_or_init(|| {
            let r = test_rootfs();
            let path = std::path::Path::new(&r);
            if path.join("bin/busybox").exists() {
                return Some(r);
            }
            let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("scripts")
                .join("download_test_rootfs.sh");
            let status = Command::new(&script).arg(&r).status().unwrap_or_default();
            if status.success() && path.join("bin/busybox").exists() {
                Some(r)
            } else {
                eprintln!("rootfs not found at {r} and download failed");
                None
            }
        })
        .clone()
}

#[test]
fn run_true_exits_zero() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, _, stderr) = run_conrt(&daemon, &["run", "--rootfs", &rootfs, "--", "/bin/true"]);
    assert!(ok, "conrt run /bin/true should exit 0, stderr: {stderr}");
}

#[test]
fn run_false_exits_nonzero() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, _, _) = run_conrt(&daemon, &["run", "--rootfs", &rootfs, "--", "/bin/false"]);
    assert!(!ok, "conrt run /bin/false should exit nonzero");
}

#[test]
fn hostname_is_conrt() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/hostname",
    ]);
    assert!(ok, "hostname command should succeed");
    assert_eq!(
        container_stdout(&stdout).trim(),
        "conrt",
        "hostname should be conrt"
    );
}

#[test]
fn uid_is_zero() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run", "--rootfs", &rootfs, "--", "/bin/sh", "-c", "id -u",
    ]);
    assert!(ok, "id -u should succeed");
    assert_eq!(
        container_stdout(&stdout).trim(),
        "0",
        "UID should be 0 inside user namespace"
    );
}

#[test]
fn proc_is_mounted() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "cat /proc/self/status | head -1",
    ]);
    assert!(ok, "cat /proc/self/status should succeed");
    assert!(!stdout.is_empty(), "proc should not be empty");
}

#[test]
fn dev_is_mounted() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, _, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "test -d /dev",
    ]);
    assert!(ok, "/dev should be a directory");
}

#[test]
fn echo_hello_world() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/echo",
        "hello from container",
    ]);
    assert!(ok, "echo should succeed");
    assert!(
        container_stdout(&stdout).contains("hello from container"),
        "stdout should contain hello message, got: {stdout:?}"
    );
}

#[test]
fn dev_null_is_writable() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, _, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "echo hello > /dev/null",
    ]);
    assert!(ok, "writing to /dev/null should succeed");
}

#[test]
fn dev_zero_is_readable() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, _, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "dd if=/dev/zero bs=1 count=4 2>/dev/null | wc -c",
    ]);
    assert!(ok, "reading from /dev/zero should succeed");
}

#[test]
fn lo_is_up() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, _, stderr) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "ip link show lo 2>&1 | grep -q 'LOOPBACK,UP'",
    ]);
    assert!(
        ok,
        "lo should be UP in the container netns, stderr: {stderr}"
    );
}

#[test]
fn sh_invocation_with_dash_c() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run", "--rootfs", &rootfs, "--", "/bin/sh", "-c", "echo ok",
    ]);
    assert!(ok, "/bin/sh -c 'echo ok' should succeed");
    assert!(stdout.contains("ok"), "stdout should contain ok");
}

#[test]
fn net_pid_invalid_pid_fails() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, _, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--net-pid",
        "1",
        "--",
        "/bin/true",
    ]);
    assert!(!ok, "joining an unrelated PID should fail");
}

#[test]
fn net_pid_joins_container_netns() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let socket = daemon.socket();
    let sock_arg = format!("--socket-path={}", socket.to_str().unwrap());
    let conrt = conrt_binary().to_str().unwrap().to_string();

    let script = format!(
        // Start a detached sandbox container (prints PID via --detach).
        // Then run a joiner container with --net-pid.
        "C=$({conrt} run {sock_arg} --detach --rootfs {rootfs} -- /bin/sleep inf) && sleep 0.3 && \
         {conrt} run {sock_arg} --rootfs {rootfs} --net-pid $C -- /bin/sh -c 'echo netns_joined' \
         2>&1; EC=$?; kill $C 2>/dev/null; exit $EC",
        conrt = conrt,
        sock_arg = sock_arg,
        rootfs = rootfs,
    );

    let output = Command::new("sh").args(["-c", &script]).output().unwrap();

    let ok = output.status.success();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("stdout: {stdout:?}");
    eprintln!("stderr: {stderr:?}");
    assert!(ok, "join should succeed, stderr: {stderr}");
    assert!(
        stdout.contains("netns_joined"),
        "stdout should contain netns_joined, got: {stdout:?}"
    );
}

#[test]
fn net_pid_localhost_communication() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let socket = daemon.socket();
    let sock_arg = format!("--socket-path={}", socket.to_str().unwrap());
    let conrt = conrt_binary().to_str().unwrap().to_string();

    let script = format!(
        // Start a detached server container (prints PID via --detach).
        // Then run a client that joins its netns and connects to localhost.
        "C=$({conrt} run {sock_arg} --detach --rootfs {rootfs} -- /bin/sh -c 'while true; do echo \
         pong | /bin/busybox nc -lp 9999; done') && sleep 0.5 && {conrt} run {sock_arg} --rootfs \
         {rootfs} --net-pid $C -- /bin/sh -c '/bin/busybox nc -w 3 127.0.0.1 9999' 2>&1; EC=$?; \
         kill $C 2>/dev/null; exit $EC",
        conrt = conrt,
        sock_arg = sock_arg,
        rootfs = rootfs,
    );

    let output = Command::new("sh").args(["-c", &script]).output().unwrap();

    let ok = output.status.success();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("stdout: {stdout:?}");
    eprintln!("stderr: {stderr:?}");
    assert!(
        ok,
        "localhost communication should succeed, stderr: {stderr}"
    );
    assert!(
        stdout.contains("pong"),
        "stdout should contain pong, got: {stdout:?}"
    );
}

#[test]
fn overlay_write_file() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "echo 'hello overlay' > /tmp/overlay_write_test && cat /tmp/overlay_write_test",
    ]);
    assert!(ok, "write file in overlay should succeed");
    assert!(
        container_stdout(&stdout).contains("hello overlay"),
        "should read back written content, got: {stdout:?}"
    );
}

#[test]
fn overlay_default_rm_discards_changes() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();

    // First run: write a file into the overlay
    let (ok1, _, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "echo 'persistent data' > /tmp/overlay_rm_test",
    ]);
    assert!(ok1, "first run should succeed");

    // Second run: the file should NOT exist (new overlay, --rm is default)
    let (ok2, stdout2, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "cat /tmp/overlay_rm_test 2>&1 || echo 'FILE_NOT_FOUND'",
    ]);
    assert!(ok2, "second run should succeed");
    assert!(
        container_stdout(&stdout2).contains("FILE_NOT_FOUND"),
        "file should not persist across --rm runs, got: {stdout2:?}"
    );
}

#[test]
fn overlay_save_does_not_break() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--save",
        "--",
        "/bin/sh",
        "-c",
        "echo 'save test ok' > /tmp/overlay_save_test && cat /tmp/overlay_save_test",
    ]);
    assert!(ok, "overlay with --save should succeed");
    assert!(
        container_stdout(&stdout).contains("save test ok"),
        "should read back written content, got: {stdout:?}"
    );
}

#[test]
fn follow_two_clients_receive_all_output() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let socket = daemon.socket();
    let socket_str = socket.to_str().unwrap().to_string();
    let conrt = conrt_binary();

    let (ok, stdout, _) = run_conrt(&daemon, &[
        "run",
        "--detach",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "echo alpha; echo beta; echo gamma; sleep 60",
    ]);
    assert!(ok, "run should succeed");
    let pid: i32 = stdout.trim().parse().expect("stdout should be a PID");

    // Give the container a moment to produce output.
    std::thread::sleep(Duration::from_millis(50));

    let mut f1 = Command::new(&conrt)
        .args([
            "logs",
            "--socket-path",
            &socket_str,
            "--follow",
            &pid.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("follower 1 should start");
    let mut f2 = Command::new(&conrt)
        .args([
            "logs",
            "--socket-path",
            &socket_str,
            "--follow",
            &pid.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("follower 2 should start");

    // Give followers time to subscribe and receive backlog.
    std::thread::sleep(Duration::from_millis(50));

    // Kill the container — triggers close_all_pipes, followers see EOF.
    let (ok, _, _) = run_conrt(&daemon, &["kill", &pid.to_string()]);
    assert!(ok, "kill should succeed");

    let deadline = Instant::now() + Duration::from_secs(5);

    let out1 = wait_for_child(&mut f1, deadline);
    let out2 = wait_for_child(&mut f2, deadline);

    assert!(out1.contains("alpha"), "follower 1 should see alpha");
    assert!(out1.contains("beta"), "follower 1 should see beta");
    assert!(out1.contains("gamma"), "follower 1 should see gamma");
    assert!(out2.contains("alpha"), "follower 2 should see alpha");
    assert!(out2.contains("beta"), "follower 2 should see beta");
    assert!(out2.contains("gamma"), "follower 2 should see gamma");
}

fn wait_for_child(child: &mut Child, deadline: Instant) -> String {
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                assert!(status.success(), "follower should exit 0");
                let mut output = String::new();
                if let Some(ref mut stdout) = child.stdout {
                    stdout.read_to_string(&mut output).unwrap();
                }
                return output;
            }
            Ok(None) => {
                assert!(
                    Instant::now() < deadline,
                    "follower did not finish within timeout"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("follower wait error: {e}"),
        }
    }
}
