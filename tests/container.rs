use std::process::Command;
use std::sync::OnceLock;

/// Path where we expect the Alpine test rootfs to be.
/// Override with `CONRT_TEST_ROOTFS` environment variable.
fn test_rootfs() -> String {
    std::env::var("CONRT_TEST_ROOTFS").unwrap_or_else(|_| "/tmp/alpine".into())
}

fn conrt_binary() -> std::path::PathBuf {
    let mut path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("target");
    path.push("debug");
    path.push("conrt");
    path
}

fn run_conrt(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(conrt_binary()).args(args).output().unwrap();
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
            let script = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
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
    let (ok, _, stderr) = run_conrt(&["run", "--rootfs", &rootfs, "--", "/bin/true"]);
    assert!(ok, "conrt run /bin/true should exit 0, stderr: {stderr}");
}

#[test]
fn run_false_exits_nonzero() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let (ok, _, _) = run_conrt(&["run", "--rootfs", &rootfs, "--", "/bin/false"]);
    assert!(!ok, "conrt run /bin/false should exit nonzero");
}

#[test]
fn hostname_is_conrt() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let (ok, stdout, _) = run_conrt(&["run", "--rootfs", &rootfs, "--", "/bin/hostname"]);
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
    let (ok, stdout, _) = run_conrt(&["run", "--rootfs", &rootfs, "--", "/bin/sh", "-c", "id -u"]);
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
    let (ok, stdout, _) = run_conrt(&[
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
    let (ok, _, _) = run_conrt(&[
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
    let (ok, stdout, _) = run_conrt(&[
        "run",
        "--rootfs",
        &rootfs,
        "--",
        "/bin/echo",
        "hello from container",
    ]);
    assert!(ok, "echo should succeed");
    assert!(
        stdout.contains("hello from container"),
        "stdout should contain hello message, got: {stdout:?}"
    );
}

#[test]
fn sh_invocation_with_dash_c() {
    let Some(rootfs) = ensure_rootfs() else {
        return;
    };
    let (ok, stdout, _) =
        run_conrt(&["run", "--rootfs", &rootfs, "--", "/bin/sh", "-c", "echo ok"]);
    assert!(ok, "/bin/sh -c 'echo ok' should succeed");
    assert!(stdout.contains("ok"), "stdout should contain ok");
}
