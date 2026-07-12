#![feature(unix_kill_process_group)]
#![feature(unix_send_signal)]

use std::io::Read;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::FromRawFd;
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixDatagram;
use std::os::unix::process::ChildExt;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
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

pub struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        self.kill();
    }
}

impl Daemon {
    pub fn new(socket: &Path) -> Self {
        let child = Self::start_daemon(socket);
        Self(child)
    }

    fn start_daemon(socket: &Path) -> Child {
        std::fs::remove_file(socket).ok();
        let dir = socket.parent().unwrap();
        std::fs::create_dir_all(dir).unwrap();

        let child = Command::new(conrt_binary())
            .args(["daemon", "--socket-path", socket.to_str().unwrap()])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
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

    pub fn kill(&mut self) {
        self.0.kill_process_group().ok();
        self.0.kill().ok();
    }

    pub fn kill_and_wait(mut self) {
        self.kill();
        let _ = self.0.wait();
    }
}

fn send_request(socket: &PathBuf, payload: &[u8]) -> Vec<u8> {
    let datagram = UnixDatagram::unbound().unwrap();
    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as _;
    let ret = unsafe {
        libc::bind(
            datagram.as_raw_fd(),
            &addr as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sa_family_t>() as _,
        )
    };
    assert_eq!(
        ret,
        0,
        "empty bind failed: {}",
        std::io::Error::last_os_error()
    );
    std::thread::sleep(std::time::Duration::from_millis(100));
    datagram.send_to(payload, socket).unwrap();

    // Peek to learn the exact response size.
    let fd = datagram.as_raw_fd();
    let n = unsafe {
        libc::recv(
            fd,
            std::ptr::null_mut(),
            0,
            libc::MSG_PEEK | libc::MSG_TRUNC,
        )
    };
    assert!(n >= 0, "peek failed: {}", std::io::Error::last_os_error());
    let mut buf = vec![0u8; n as usize];
    datagram.recv(&mut buf).unwrap();
    buf
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
    let daemon = Daemon::new(&socket);

    let resp = send_request(&socket, b"{}");
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(!v["ok"].as_bool().unwrap());
    assert!(v["error"].as_str().unwrap().contains("invalid request"));

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn protocol_unknown_type() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    let resp = send_request(&socket, br#"{"type":"Unknown"}"#);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(!v["ok"].as_bool().unwrap());
    assert!(v["error"].as_str().unwrap().contains("unknown variant"));

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn list_returns_empty_initially() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    let resp = send_request(&socket, br#"{"type":"List"}"#);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(v["containers"].as_array().unwrap().len(), 0);

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn kill_unknown_container_returns_error() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    let resp = send_request(&socket, br#"{"type":"Kill","pid":99999}"#);
    let v: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(!v["ok"].as_bool().unwrap());
    assert!(v["error"].as_str().unwrap().contains("not found"));

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn run_and_list_and_kill_flow() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    let (run_ok, stdout, stderr) = run_conrt(&[
        "run",
        "--detach",
        "--socket-path",
        socket.to_str().unwrap(),
        "--",
        "/bin/sleep",
        "10",
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

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn detach_cli_output_is_pid() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    let (ok, stdout, stderr) = run_conrt(&[
        "run",
        "--detach",
        "--socket-path",
        socket.to_str().unwrap(),
        "--",
        "/bin/sleep",
        "10",
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

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
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
    let daemon = Daemon::new(&socket);

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

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

fn subscribe_and_receive_fd(socket: &Path, pid: i32) -> Option<std::os::unix::io::RawFd> {
    let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    assert!(fd >= 0, "socket creation failed");

    // Set 3s recv timeout so tests don't hang forever.
    let tv = libc::timeval {
        tv_sec: 3,
        tv_usec: 0,
    };
    let ret = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const _ as *const _,
            std::mem::size_of::<libc::timeval>() as _,
        )
    };
    assert_eq!(ret, 0, "setsockopt rcvtimeo failed");

    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    addr.sun_family = libc::AF_UNIX as _;
    let ret = unsafe {
        libc::bind(
            fd,
            &addr as *const _ as *const _,
            std::mem::size_of::<libc::sa_family_t>() as _,
        )
    };
    assert_eq!(ret, 0, "bind failed: {}", std::io::Error::last_os_error());

    let req = format!(r#"{{"type":"Logs","pid":{pid},"stream":true}}"#);
    let socket_c = std::ffi::CString::new(socket.to_str().unwrap()).unwrap();
    let mut dest: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    dest.sun_family = libc::AF_UNIX as _;
    unsafe {
        std::ptr::copy_nonoverlapping(
            socket_c.as_ptr(),
            dest.sun_path.as_mut_ptr() as *mut i8,
            socket_c.as_bytes().len(),
        );
    }

    let addr_len = std::mem::size_of::<libc::sa_family_t>() + socket_c.as_bytes_with_nul().len();
    let ret = unsafe {
        libc::connect(
            fd,
            &dest as *const _ as *const libc::sockaddr,
            addr_len as _,
        )
    };
    assert_eq!(
        ret,
        0,
        "connect failed: {}",
        std::io::Error::last_os_error()
    );
    eprintln!("subscribe: connected, sending request");
    let ret = unsafe { libc::send(fd, req.as_ptr() as *const _, req.len(), 0) };
    assert_eq!(
        ret,
        req.len() as isize,
        "send failed: {}",
        std::io::Error::last_os_error()
    );
    eprintln!("subscribe: sent, waiting for fd-pass");

    #[repr(align(8))]
    #[derive(Default)]
    struct CmsgStackBuffer {
        // Statically sized to fit our headers and payload
        bytes: [u8; unsafe { libc::CMSG_SPACE(std::mem::size_of::<RawFd>() as _) as usize }],
    }

    let mut cmsg_buf = CmsgStackBuffer::default();
    let mut data_buf = vec![0u8; 4096];
    let mut iov = libc::iovec {
        iov_base: data_buf.as_mut_ptr() as *mut _,
        iov_len: data_buf.len(),
    };
    let mut msghdr: libc::msghdr = unsafe { std::mem::zeroed() };
    msghdr.msg_iov = &mut iov;
    msghdr.msg_iovlen = 1;
    msghdr.msg_control = cmsg_buf.bytes.as_mut_ptr() as *mut _;
    msghdr.msg_controllen = cmsg_buf.bytes.len() as _;
    let ret = unsafe { libc::recvmsg(fd, &mut msghdr, 0) };
    if ret < 0 {
        let err = std::io::Error::last_os_error();
        eprintln!("subscribe: recvmsg err={err}");
        let _ = unsafe { libc::close(fd) };
        return None;
    }
    eprintln!("subscribe: recvmsg ret={ret}");
    if ret > 0 {
        eprintln!(
            "subscribe: payload={:?}",
            String::from_utf8_lossy(&data_buf[..ret as usize])
        );
    }

    unsafe {
        let cmsg_hdr = cmsg_buf.bytes.as_ptr() as *const libc::cmsghdr;
        if (*cmsg_hdr).cmsg_len == 0 {
            eprintln!("subscribe: no cmsg (plain datagram reply)");
            let _ = libc::close(fd);
            return None;
        }
        if (*cmsg_hdr).cmsg_level != libc::SOL_SOCKET || (*cmsg_hdr).cmsg_type != libc::SCM_RIGHTS {
            eprintln!(
                "subscribe: unexpected cmsg level={} type={}",
                (*cmsg_hdr).cmsg_level,
                (*cmsg_hdr).cmsg_type
            );
            let _ = libc::close(fd);
            return None;
        }
        let data_ptr = libc::CMSG_DATA(cmsg_hdr);
        let rcvd_fd = data_ptr.cast::<RawFd>().read();
        let _ = libc::close(fd);
        eprintln!("subscribe: received fd={rcvd_fd}");
        Some(rcvd_fd)
    }
}

#[test]
fn subscribe_returns_container_output() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    // Start a container that prints lines and stays alive.
    let (ok, stdout, stderr) = run_conrt(&[
        "run",
        "--detach",
        "--socket-path",
        socket.to_str().unwrap(),
        "--",
        "/bin/sh",
        "-c",
        "echo hello; echo world; sleep 10",
    ]);
    assert!(ok, "run should succeed, stderr: {stderr}");
    let pid: i32 = stdout.trim().parse().expect("stdout should be a PID");

    // Give the container a moment to produce output (hello\nworld\n).
    std::thread::sleep(Duration::from_millis(10));

    let pipe_fd =
        subscribe_and_receive_fd(&socket, pid).expect("should receive pipe fd within timeout");

    let mut pipe_reader = unsafe { std::fs::File::from_raw_fd(pipe_fd) };

    // Clean up.
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
            panic!("timeout waiting for container to die");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let mut output = String::new();
    pipe_reader.read_to_string(&mut output).ok();

    assert!(
        output.contains("hello"),
        "pipe output should contain 'hello', got: {output:?}"
    );
    assert!(
        output.contains("world"),
        "pipe output should contain 'world', got: {output:?}"
    );

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn subscribe_unknown_container_returns_error() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    let pipe_fd = subscribe_and_receive_fd(&socket, 99999);
    assert!(pipe_fd.is_none(), "subscribe to unknown pid should fail");

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn subscribe_two_clients_same_container() {
    let dir = test_dir();
    let socket = dir.join("conrt.sock");
    let daemon = Daemon::new(&socket);

    let (ok, stdout, stderr) = run_conrt(&[
        "run",
        "--detach",
        "--socket-path",
        socket.to_str().unwrap(),
        "--",
        "/bin/sh",
        "-c",
        "echo alpha; echo beta; sleep 10",
    ]);
    assert!(ok, "run should succeed, stderr: {stderr}");
    let pid: i32 = stdout.trim().parse().expect("stdout should be a PID");

    // Give the container a moment to produce output.
    std::thread::sleep(Duration::from_millis(10));

    // Two concurrent subscribers.
    let pipe1 = subscribe_and_receive_fd(&socket, pid).expect("sub1 should receive pipe fd");
    let pipe2 = subscribe_and_receive_fd(&socket, pid).expect("sub2 should receive pipe fd");

    let mut reader1 = unsafe { std::fs::File::from_raw_fd(pipe1) };
    let mut reader2 = unsafe { std::fs::File::from_raw_fd(pipe2) };

    // Kill container and give pipes time to drain.
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
            panic!("timeout waiting for container to die");
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    let mut out1 = String::new();
    let mut out2 = String::new();
    reader1.read_to_string(&mut out1).ok();
    reader2.read_to_string(&mut out2).ok();

    for (out, label) in [(&out1, "sub1"), (&out2, "sub2")] {
        assert!(
            out.contains("alpha"),
            "{label} output should contain 'alpha', got: {out:?}"
        );
        assert!(
            out.contains("beta"),
            "{label} output should contain 'beta', got: {out:?}"
        );
    }

    daemon.kill_and_wait();
    std::fs::remove_dir_all(&dir).ok();
}
