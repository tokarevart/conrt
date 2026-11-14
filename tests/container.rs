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
        "C=$({conrt} run {sock_arg} --detach --rootfs {rootfs} -- /bin/sleep inf) && sleep 0.1 && \
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
         pong | /bin/busybox nc -lp 9999; done') && sleep 0.1 && {conrt} run {sock_arg} --rootfs \
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

/// Starts a detached container and returns its container PID.
fn start_detached(daemon: &Daemon, args: &[&str]) -> i32 {
    let mut full = vec!["run", "--detach"];
    full.extend_from_slice(args);
    let (ok, stdout, stderr) = run_conrt(daemon, &full);
    assert!(ok, "detached run failed: {stderr}");
    container_stdout(&stdout)
        .trim()
        .lines()
        .last()
        .expect("detached run should print a pid")
        .trim()
        .parse()
        .expect("detached run pid should be numeric")
}

/// Host PIDs of `pid`'s direct children, via /proc/.../children.
fn children_of(pid: i32) -> Vec<i32> {
    std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children"))
        .ok()
        .map(|s| {
            s.split_whitespace()
                .filter_map(|p| p.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

fn gone_or_zombie(pid: i32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Err(_) => true,
        Ok(stat) => {
            stat.rsplit(')')
                .next()
                .and_then(|rest| rest.split_whitespace().next())
                == Some("Z")
        }
    }
}

#[test]
fn net_pid_join_is_repeatable() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let parent = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let parent_arg = parent.to_string();

    for tag in ["first_join", "second_join"] {
        let (ok, stdout, stderr) = run_conrt(&daemon, &[
            "run",
            "--rootfs",
            &rootfs,
            "--net-pid",
            &parent_arg,
            "--",
            "/bin/sh",
            "-c",
            &format!("echo {tag}"),
        ]);
        assert!(ok, "join #{tag} into the same container failed: {stderr}");
        assert!(
            container_stdout(&stdout).contains(tag),
            "{tag}: unexpected output: {stdout:?}"
        );
    }

    let _ = run_conrt(&daemon, &["kill", &parent_arg]);
}

#[test]
fn net_pid_joins_containers_created_before_the_first_join() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let a = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let b = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let (a, b) = (a.to_string(), b.to_string());

    for (tag, parent) in [("joined_a", &a), ("joined_b", &b)] {
        let (ok, stdout, stderr) = run_conrt(&daemon, &[
            "run",
            "--rootfs",
            &rootfs,
            "--net-pid",
            parent,
            "--",
            "/bin/sh",
            "-c",
            &format!("echo {tag}"),
        ]);
        assert!(ok, "join into {parent} failed: {stderr}");
        assert!(container_stdout(&stdout).contains(tag));
    }

    let _ = run_conrt(&daemon, &["kill", &a]);
    let _ = run_conrt(&daemon, &["kill", &b]);
}

#[test]
fn net_pid_join_shares_netns_and_keeps_isolation() {
    use std::os::unix::fs::MetadataExt;

    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let parent = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let parent_arg = parent.to_string();
    let parent_netns = std::fs::metadata(format!("/proc/{parent}/ns/net"))
        .unwrap()
        .ino();

    let (ok, stdout, stderr) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--net-pid",
        &parent_arg,
        "--no-init",
        "--",
        "/bin/sh",
        "-c",
        "echo pid=$$; echo host=$(hostname); echo ns=$(stat -Lc %i /proc/self/ns/net); test -r \
         /proc/self/stat && echo proc-ok",
    ]);
    assert!(ok, "joined run failed: {stderr}");
    let out = container_stdout(&stdout);
    assert!(
        out.contains("pid=1"),
        "joined container should be PID 1: {out}"
    );
    assert!(out.contains("host=conrt"), "UTS should be isolated: {out}");
    assert!(
        out.contains(&format!("ns={parent_netns}")),
        "joined container should share the parent netns inode {parent_netns}: {out}"
    );
    assert!(out.contains("proc-ok"), "/proc should be mounted: {out}");

    let _ = run_conrt(&daemon, &["kill", &parent_arg]);
}

#[test]
fn net_pid_does_not_move_the_daemon() {
    use std::os::unix::fs::MetadataExt;

    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let parent = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let parent_arg = parent.to_string();

    let (ok, _, stderr) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--net-pid",
        &parent_arg,
        "--",
        "/bin/true",
    ]);
    assert!(ok, "join failed: {stderr}");

    let host = std::fs::metadata("/proc/self/ns/net").unwrap();
    let daemon_ns = std::fs::metadata(format!("/proc/{}/ns/net", daemon.0.id())).unwrap();
    assert_eq!(
        (host.dev(), host.ino()),
        (daemon_ns.dev(), daemon_ns.ino()),
        "the daemon must stay in the host netns after a join"
    );

    let _ = run_conrt(&daemon, &["kill", &parent_arg]);
}

#[test]
fn net_pid_kill_tears_down_the_joined_container() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let parent = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let parent_arg = parent.to_string();
    let joiner = start_detached(&daemon, &[
        "--rootfs",
        &rootfs,
        "--net-pid",
        &parent_arg,
        "--",
        "/bin/sleep",
        "60",
    ]);
    let joiner_arg = joiner.to_string();

    // shim -> payload (PID 1 in the container pidns) -> user command.
    // The daemon replies as soon as the shim is forked, so poll briefly.
    let deadline = Instant::now() + Duration::from_secs(2);
    let payload = loop {
        if let Some(pid) = children_of(joiner).first() {
            break *pid;
        }
        assert!(
            Instant::now() < deadline,
            "the join shim did not fork a payload"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    let user_cmd = loop {
        if let Some(pid) = children_of(payload).first() {
            break Some(*pid);
        }
        if Instant::now() >= deadline {
            break None;
        }
        std::thread::sleep(Duration::from_millis(10));
    };

    let (ok, _, stderr) = run_conrt(&daemon, &["kill", &joiner_arg]);
    assert!(ok, "kill failed: {stderr}");

    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let payload_gone = gone_or_zombie(payload);
        let user_gone = user_cmd.is_none_or(gone_or_zombie);
        if payload_gone && user_gone {
            let _ = run_conrt(&daemon, &["kill", &parent_arg]);
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("payload {payload} / user command {user_cmd:?} survived kill of the joined container");
}

#[test]
fn net_pid_join_propagates_exit_code() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let parent = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let parent_arg = parent.to_string();

    let output = Command::new(conrt_binary())
        .args([
            "run",
            "--socket-path",
            daemon.socket().to_str().unwrap(),
            "--rootfs",
            &rootfs,
            "--net-pid",
            &parent_arg,
            "--",
            "/bin/sh",
            "-c",
            "exit 42",
        ])
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(42),
        "exit code should survive the shim, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = run_conrt(&daemon, &["kill", &parent_arg]);
}

#[test]
fn net_pid_chained_join() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();
    let a = start_detached(&daemon, &["--rootfs", &rootfs, "--", "/bin/sleep", "60"]);
    let a_arg = a.to_string();
    // A container that is itself a joiner lives in A's namespaces, so a join
    // of that joiner must land in A's netns too.
    let b = start_detached(&daemon, &[
        "--rootfs",
        &rootfs,
        "--net-pid",
        &a_arg,
        "--",
        "/bin/sleep",
        "60",
    ]);
    let b_arg = b.to_string();

    let (ok, stdout, stderr) = run_conrt(&daemon, &[
        "run",
        "--rootfs",
        &rootfs,
        "--net-pid",
        &b_arg,
        "--",
        "/bin/sh",
        "-c",
        "echo chained",
    ]);
    assert!(ok, "chained join failed: {stderr}");
    assert!(container_stdout(&stdout).contains("chained"));

    let _ = run_conrt(&daemon, &["kill", &a_arg]);
    let _ = run_conrt(&daemon, &["kill", &b_arg]);
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

#[test]
fn detached_stdin_is_devnull_cat_exits_immediately() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();

    // `cat` reads from stdin until EOF. With stdin wired to /dev/null
    // (our fix), cat gets EOF immediately and exits with status 0.
    let (ok, stdout, stderr) = run_conrt(&daemon, &[
        "run", "--detach", "--rootfs", &rootfs, "--", "/bin/cat",
    ]);
    assert!(ok, "detached run should succeed, stderr: {stderr}");
    let pid: i32 = stdout.trim().parse().expect("stdout should be a PID");

    // Poll `list` until the container disappears (it exited, reaped).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, list_out, _) = run_conrt(&daemon, &["list"]);
        if !list_out.contains(&pid.to_string()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "container {pid} (cat) did not exit within timeout"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // Logs should succeed (container is in graveyard).
    let (ok, _, _) = run_conrt(&daemon, &["logs", &pid.to_string()]);
    assert!(ok, "logs should succeed after container exits");

    // Best-effort kill in case it's still alive.
    let _ = run_conrt(&daemon, &["kill", &pid.to_string()]);
}

#[test]
fn detached_shell_loop_produces_all_output() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let daemon = Daemon::new();

    // This is the pattern the user reported: a shell loop that echoes output
    // periodically. Without the /dev/null stdin fix, commands that check stdin
    // could fail. More importantly, this validates that a long-lived detached
    // container runs to completion and all output is captured.
    let (ok, stdout, stderr) = run_conrt(&daemon, &[
        "run",
        "--detach",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/sh",
        "-c",
        "for i in 1 2 3; do echo line_$i; done",
    ]);
    assert!(ok, "detached run should succeed, stderr: {stderr}");
    let pid: i32 = stdout.trim().parse().expect("stdout should be a PID");

    // Poll until the container exits (loop finishes near-instantly).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (_, list_out, _) = run_conrt(&daemon, &["list"]);
        if !list_out.contains(&pid.to_string()) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "container {pid} (shell loop) did not finish within timeout"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // All output lines must be captured in the log graveyard.
    let (ok, logs_stdout, _) = run_conrt(&daemon, &["logs", &pid.to_string()]);
    assert!(ok, "logs should succeed for exited container");
    assert!(
        logs_stdout.contains("line_1"),
        "logs should contain line_1, got: {logs_stdout:?}"
    );
    assert!(
        logs_stdout.contains("line_2"),
        "logs should contain line_2, got: {logs_stdout:?}"
    );
    assert!(
        logs_stdout.contains("line_3"),
        "logs should contain line_3, got: {logs_stdout:?}"
    );
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
