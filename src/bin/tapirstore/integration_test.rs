use std::io::Cursor;

fn run_script(script: &str) -> (String, String, i32) {
    let dir = tempfile::TempDir::new().unwrap();
    let full_script = format!("open {}; {script}", dir.path().display());
    run_raw_script(&full_script)
}

fn run_script_with_dir(dir: &std::path::Path, script: &str) -> (String, String, i32) {
    let full_script = format!("open {}; {script}", dir.display());
    run_raw_script(&full_script)
}

fn run_raw_script(script: &str) -> (String, String, i32) {
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let args = vec!["tapirstore".to_string(), script.to_string()];
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

#[test]
fn test_put_get_roundtrip() {
    let (stdout, stderr, code) = run_script("put k1 v1 5; get k1");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "k1=v1 @5\n");
}

#[test]
fn test_prepare_commit_get() {
    let (stdout, stderr, code) =
        run_script("prepare 1:1 5 x=v1 y=v2; commit 1:1 5; get-at x 5; get-at y 5");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "x=v1 @5\ny=v2 @5\n");
}

#[test]
fn test_scan() {
    let (stdout, stderr, code) =
        run_script("put a v1 5; put b v2 10; put c v3 15; scan a d 20");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "a=v1 @5\nb=v2 @10\nc=v3 @15\n");
}

#[test]
fn test_status_fresh() {
    let (stdout, stderr, code) = run_script("status");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "view=0 sealed_segments=0\n");
}

#[test]
fn test_seal_and_list_vlogs() {
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 x=v1; commit 1:1 5; seal; status; list-vlogs",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    // After seal with default 256KB threshold, data stays in active segment.
    // Active segment has views=[0,1]. Size is the VLog data for 2 IR entries
    // (Prepare + Commit) — 138 bytes for single-key prepare "x=v1".
    assert_eq!(
        stdout,
        "view=1 sealed_segments=0\n\
         vlog_seg_0000 size=138 views=[0,1]\n"
    );
}

#[test]
fn test_get_range() {
    let (stdout, stderr, code) =
        run_script("put k v1 5; put k v2 20; get-range k 5");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "write_ts=5 next_ts=20\n");
}

#[test]
fn test_has_writes() {
    let (stdout, stderr, code) =
        run_script("put x v1 10; has-writes a z 0 100; has-writes a z 50 100");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "true\nfalse\n");
}

#[test]
fn test_delete() {
    let (stdout, stderr, code) =
        run_script("put k v1 5; delete k 10; get-at k 10; get-at k 5");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "k=<none> @10\nk=v1 @5\n");
}

#[test]
fn test_get_nonexistent() {
    let (stdout, stderr, code) = run_script("get x");
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "x=<none> @0\n");
}

#[test]
fn test_multi_view() {
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 a=v1; commit 1:1 5; seal; \
         prepare 1:2 10 b=v2; commit 1:2 10; seal; \
         scan a z 20; status",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "a=v1 @5\nb=v2 @10\n\
         view=2 sealed_segments=0\n"
    );
}

#[test]
fn test_error_before_open() {
    let (stdout, stderr, code) = run_raw_script("get x");
    assert_eq!(stdout, "");
    assert!(
        stderr.contains("no store open"),
        "stderr should contain 'no store open', got: {stderr}"
    );
    assert_eq!(code, 1);
}

#[test]
fn test_open_with_small_segments() {
    let dir = tempfile::TempDir::new().unwrap();
    let script = format!(
        "open-with {} 64; \
         prepare 1:1 5 x=v1; commit 1:1 5; seal; \
         prepare 1:2 10 y=v2; commit 1:2 10; seal; \
         status; list-vlogs",
        dir.path().display(),
    );
    let (stdout, stderr, code) = run_raw_script(&script);
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    // With 64-byte threshold, each seal rotates the segment.
    // After 2 seals: 2 sealed segments (0, 1) + 1 active (2).
    // Each single-key prepare+commit → 138 bytes per segment.
    assert_eq!(
        stdout,
        "view=2 sealed_segments=2\n\
         vlog_seg_0002 size=0 views=[2]\n\
         vlog_seg_0000 size=138 views=[0]\n\
         vlog_seg_0001 size=138 views=[1]\n"
    );
}

#[test]
fn test_prepare_commit_survives_seal() {
    // Verify that prepare+commit data is readable after seal (OnDisk resolution)
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 a=val_a b=val_b; commit 1:1 5; \
         get a; get b; seal; \
         get a; get b; get-at a 5; get-at b 5",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "a=val_a @5\nb=val_b @5\n\
         a=val_a @5\nb=val_b @5\n\
         a=val_a @5\nb=val_b @5\n"
    );
}

#[test]
fn test_scan_after_seal() {
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 a=v1 b=v2 c=v3; commit 1:1 5; seal; scan a d 10",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "a=v1 @5\nb=v2 @5\nc=v3 @5\n");
}

#[test]
fn test_cross_view_scan() {
    // Data committed across views, scan returns all
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 a=v1; commit 1:1 5; seal; \
         prepare 1:2 10 b=v2; commit 1:2 10; seal; \
         prepare 1:3 15 c=v3; commit 1:3 15; \
         scan a d 20",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "a=v1 @5\nb=v2 @10\nc=v3 @15\n");
}

#[test]
fn test_multi_version_key() {
    // Same key written at multiple timestamps
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 k=v1; commit 1:1 5; \
         prepare 1:2 20 k=v2; commit 1:2 20; \
         get k; get-at k 5; get-at k 15; get-at k 20; get-range k 5",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "k=v2 @20\n\
         k=v1 @5\n\
         k=v1 @5\n\
         k=v2 @20\n\
         write_ts=5 next_ts=20\n"
    );
}

#[test]
fn test_reopen_after_seal() {
    // Verify that a sealed store can be reopened and data is still readable
    let dir = tempfile::TempDir::new().unwrap();

    // Session 1: commit data and seal
    let (stdout1, stderr1, code1) = run_script_with_dir(
        dir.path(),
        "prepare 1:1 5 x=hello y=world; commit 1:1 5; seal; status",
    );
    assert_eq!(stderr1, "");
    assert_eq!(code1, 0);
    assert_eq!(stdout1, "view=1 sealed_segments=0\n");

    // Session 2: reopen and read
    let (stdout2, stderr2, code2) =
        run_script_with_dir(dir.path(), "status; list-vlogs");
    assert_eq!(stderr2, "");
    assert_eq!(code2, 0);
    // Reopened store restores view and write offset from manifest.
    // Active segment retains its size (150 bytes for 2-key prepare+commit).
    // Views=[1] because start_view(1) is called at open time.
    assert_eq!(
        stdout2,
        "view=1 sealed_segments=0\n\
         vlog_seg_0000 size=150 views=[1]\n"
    );
}

#[test]
fn test_unknown_command() {
    let (stdout, stderr, code) = run_script("badcmd");
    assert_eq!(stdout, "");
    assert!(
        stderr.contains("unknown command: badcmd"),
        "stderr should contain 'unknown command: badcmd', got: {stderr}"
    );
    assert_eq!(code, 1);
}

#[test]
fn test_stdin_input() {
    let dir = tempfile::TempDir::new().unwrap();
    let script = format!(
        "open {}\nput x hello 5\nget x\nstatus",
        dir.path().display()
    );
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let args = vec!["tapirstore".to_string()]; // no script arg → read stdin
    let code = tapirs::unified::cli::run(
        args,
        Cursor::new(script.into_bytes()),
        &mut stdout,
        &mut stderr,
    );
    let stdout = String::from_utf8(stdout).unwrap();
    let stderr = String::from_utf8(stderr).unwrap();
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(stdout, "x=hello @5\nview=0 sealed_segments=0\n");
}

#[test]
fn test_dump_vlog() {
    // Prepare+commit single-key, seal, then dump the VLog segment.
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 x=v1; commit 1:1 5; seal; \
         list-vlogs; dump-vlog 0",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "vlog_seg_0000 size=138 views=[0,1]\n\
         @0 op=1:1 PREPARE txn=1:1 ts=5 x=v1\n\
         @64 op=1:2 COMMIT txn=1:1 ts=5 ref=same_view(1:1)\n"
    );
}

#[test]
fn test_dump_vlog_multi_key() {
    // Prepare+commit two keys, seal, then dump.
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 a=val1 b=val2; commit 1:1 5; seal; dump-vlog 0",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "@0 op=1:1 PREPARE txn=1:1 ts=5 a=val1 b=val2\n\
         @74 op=1:2 COMMIT txn=1:1 ts=5 ref=same_view(1:1)\n"
    );
}

#[test]
fn test_dump_vlog_multi_views() {
    // Prepare in view 0, seal, commit in view 1 → cross-view ref in same segment.
    let (stdout, stderr, code) = run_script(
        "prepare 1:1 5 x=v1; seal; commit 1:1 5; seal; dump-vlog 0",
    );
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "@0 op=1:1 PREPARE txn=1:1 ts=5 x=v1\n\
         @64 op=1:2 COMMIT txn=1:1 ts=5 ref=cross_view(v=0 seg=0 off=0 len=64)\n"
    );
}

#[test]
fn test_dump_vlog_multi_segments() {
    // With small segment threshold, seal rotates segments.
    // Prepare 1:1 in view 0 → segment 0, prepare 1:2 in view 1 → segment 1.
    // Both commits in view 2 → segment 2, each with cross-view/cross-segment
    // references back to their respective prepares.
    let dir = tempfile::TempDir::new().unwrap();
    let script = format!(
        "open-with {} 64; \
         prepare 1:1 5 x=v1; seal; \
         prepare 1:2 10 y=v2; seal; \
         commit 1:1 5; commit 1:2 10; seal; \
         list-vlogs; dump-vlog 0; dump-vlog 1; dump-vlog 2",
        dir.path().display(),
    );
    let (stdout, stderr, code) = run_raw_script(&script);
    assert_eq!(stderr, "");
    assert_eq!(code, 0);
    assert_eq!(
        stdout,
        "vlog_seg_0003 size=0 views=[3]\n\
         vlog_seg_0000 size=64 views=[0]\n\
         vlog_seg_0001 size=64 views=[1]\n\
         vlog_seg_0002 size=172 views=[2]\n\
         @0 op=1:1 PREPARE txn=1:1 ts=5 x=v1\n\
         @0 op=1:2 PREPARE txn=1:2 ts=10 y=v2\n\
         @0 op=1:3 COMMIT txn=1:1 ts=5 ref=cross_view(v=0 seg=0 off=0 len=64)\n\
         @86 op=1:4 COMMIT txn=1:2 ts=10 ref=cross_view(v=1 seg=1 off=0 len=64)\n"
    );
}
