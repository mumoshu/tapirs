use std::io::Cursor;

fn run_raw(mode: &str, script: &str) -> (String, String, i32) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let args = vec!["tapirstore".to_string(), mode.to_string(), script.to_string()];
    let code = tapirs::unified::cli::run(
        args,
        Cursor::new(Vec::<u8>::new()),
        &mut stdout,
        &mut stderr,
    );
    (
        String::from_utf8(stdout).unwrap(),
        String::from_utf8(stderr).unwrap(),
        code,
    )
}

fn run_with_tmpdir(mode: &str, script_tail: &str) -> (String, String, i32) {
    let dir = tempfile::TempDir::new().unwrap();
    let script = format!("open {}; {script_tail}", dir.path().display());
    run_raw(mode, &script)
}

#[test]
fn tapir_prepare_commit_get_roundtrip() {
    let (stdout, stderr, code) = run_with_tmpdir(
        "tapir",
        "prepare 1:1 5 x=v1 y=v2; commit 1:1 5; get-at x 5; get-at y 5",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "x=v1 @5\ny=v2 @5\n");
}

#[test]
fn tapir_scan_and_status() {
    let (stdout, stderr, code) = run_with_tmpdir(
        "tapir",
        "prepare 1:1 5 a=v1; commit 1:1 5; prepare 1:2 10 b=v2; commit 1:2 10; scan a z 10; status",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "a=v1 @5\nb=v2 @10\nview=0 sealed_segments=0\n");
}

#[test]
fn tapir_seal_list_and_dump_vlog() {
    let dir = tempfile::TempDir::new().unwrap();
    let script = format!(
        "open-with {} 0; prepare 1:1 5 x=v1; commit 1:1 5; seal; status; list-vlogs; dump-vlog 0",
        dir.path().display()
    );
    let (stdout, stderr, code) = run_raw("tapir", &script);
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "view=1 sealed_segments=1\n\
         vlog_seg_0001 size=0 views=[1]\n\
         vlog_seg_0000 size=64 views=[0]\n\
         @0 TAPIR_COMMIT txn=1:1 ts=5 x=v1\n"
    );
}

#[test]
fn ir_prepare_commit_dump_vlog() {
    let (stdout, stderr, code) = run_with_tmpdir(
        "ir",
        "prepare 1:1 5 x=v1; commit 1:1 5; seal; dump-vlog 0",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "@0 op=1:1 PREPARE txn=1:1 ts=5 x=v1\n\
         @64 op=1:2 COMMIT txn=1:1 ts=5 ref=same_view(1:1)\n"
    );
}

#[test]
fn ir_status_and_list_vlogs() {
    let (stdout, stderr, code) = run_with_tmpdir("ir", "status; list-vlogs");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "view=0 sealed_segments=0\nvlog_seg_0000 size=0 views=[0]\n");
}

#[test]
fn mode_and_open_errors() {
    let (stdout, stderr, code) = run_raw("bad", "status");
    assert_eq!(stdout, "");
    assert_eq!(code, 1);
    assert!(stderr.contains("unknown mode"));

    let (stdout, stderr, code) = run_raw("tapir", "get x");
    assert_eq!(stdout, "");
    assert_eq!(code, 1);
    assert!(stderr.contains("no store open"));
}
