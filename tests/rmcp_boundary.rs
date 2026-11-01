//! TRANSPORT-23: `scripts/check-rmcp.sh` fails if rmcp shows in a build
//! without `mcp`, or if a build with it holds anything but one rmcp 3.8.x, so
//! either fails `cargo test`.

use std::process::Command;

#[test]
fn rmcp_only_behind_mcp_and_one_version_of_it() {
    let root = env!("CARGO_MANIFEST_DIR");
    let out = Command::new("bash")
        .arg(format!("{root}/scripts/check-rmcp.sh"))
        .current_dir(root)
        .output()
        .expect("run scripts/check-rmcp.sh");
    assert!(
        out.status.success(),
        "scripts/check-rmcp.sh failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
