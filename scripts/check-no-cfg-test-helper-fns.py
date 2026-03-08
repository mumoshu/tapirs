#!/usr/bin/env python3
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SKIP_PATH_PARTS = ("/tests/", "\\tests\\")
SKIP_SUFFIXES = ("_test.rs",)
TARGET_GLOBS = (
    "src/**/*.rs",
)
EXCLUDE_PATHS = {
}

# Structs exempt from the "public struct with public fields must derive an
# API trait" check. These are infrastructure or configuration types where
# public fields are the intended API surface, or types that are `pub` only
# because Rust requires matching visibility in trait impls or public
# function signatures within otherwise-private modules.
EXEMPT_STRUCTS = {
    # Simple data holder (Arc + SocketAddr), not a data-transfer type.
    "src/node/node/mod.rs:ReplicaHandle",
    # io_uring configuration DTOs — users construct via struct literals.
    "src/transport/uring/launcher.rs:ShardAssignment",
    "src/transport/uring/launcher.rs:CoreConfig",
    # `pub` required by trait impl in private bench module.
    "src/bench/cluster.rs:BenchTarget",
    # Returned from pub functions in internal testing module.
    "src/testing/discovery.rs:TestDiscoveryCluster",
    # Internal IR record type re-exported for convenience.
    "src/unified/ir/record.rs:IrMemEntry",
    # Returned from pub method `list_backups` used by binary crate.
    "src/backup/types.rs:BackupInfo",
}

ALLOWED_UNUSED_ATTRS = {
    "src/bin/tapi/node/mod.rs:58",
    "src/ir/record.rs:50",
    "src/ir/record.rs:55",
}

# Detects `#[cfg(test)]` followed by a public function declaration (`pub fn`,
# `pub(crate) fn`, etc.). Used to catch test-only API surface leakage.
# Does NOT validate function body shape or semantics.
PUBLIC_CFG_TEST_FN_PATTERN = re.compile(
    r"""(?mxs)
    ^[ \t]*\#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\][ \t]*\n
    (?:^[ \t]*\#\s*\[[^\]]+\][ \t]*\n)*
    ^[ \t]*pub(?:\([^)]+\))?[ \t]+fn[ \t]+
    (?P<name>[A-Za-z_]\w*)
    """,
)

# Detects `#[cfg(test)]` followed by a `fn` declaration (any visibility).
# Used as a start-line anchor for the body-length scanner below.
CFG_TEST_FN_HEADER_PATTERN = re.compile(
    r"""(?mx)
    ^[ \t]*\#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\][ \t]*$
    """
)

# Detects a struct header line that opens a braced struct body.
# Only matches fully-public structs (`pub struct`, NOT `pub(crate) struct`
# or `pub(super) struct`). Private and restricted structs are exempt.
# Does NOT match tuple structs or unit structs.
STRUCT_HEADER_PATTERN = re.compile(
    r"""(?mx)
    ^[ \t]*pub[ \t]+struct[ \t]+(?P<name>[A-Za-z_]\w*)\b[^{;]*\{[ \t]*$
    """
)

# Detects a `#[derive(...)]` line and captures derive items.
DERIVE_PATTERN = re.compile(r"^\s*#\s*\[\s*derive\s*\((?P<items>[^)]*)\)\s*\]\s*$")

# Detects a field line starting with a fully-public visibility modifier
# (`pub field`). Does NOT match restricted visibility (`pub(crate) field`,
# `pub(super) field`). The `\s+` after `pub` ensures `pub(crate)` does
# not match (no whitespace between `pub` and `(`).
PUBLIC_FIELD_PATTERN = re.compile(r"^\s*pub\s+")

# Traits that indicate a struct is a genuine API type (not just an
# internal implementation detail with accidentally-public fields).
API_TRAITS = {"Clone", "Eq", "Serialize", "Deserialize"}

# Matches a `fn` declaration line (any visibility). Used to find fn headers
# after a `#[cfg(test)]` attribute.
FN_HEADER_PATTERN = re.compile(
    r"""(?mx)
    ^[ \t]*(?:pub(?:\([^)]+\))?[ \t]+)?fn[ \t]+(?P<name>[A-Za-z_]\w*)
    """
)


def is_skipped(path: Path) -> bool:
    p = str(path)
    if any(part in p for part in SKIP_PATH_PARTS):
        return True
    if p.endswith(SKIP_SUFFIXES):
        return True
    return False


def line_of_offset(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def candidate_files() -> list[Path]:
    files: set[Path] = set()
    for pattern in TARGET_GLOBS:
        files.update(ROOT.glob(pattern))
    return sorted(path for path in files if path.is_file())


def is_trivial_line(line: str) -> bool:
    """Return True if the line is empty, whitespace-only, a comment, or brace-only."""
    stripped = line.strip()
    if not stripped:
        return True
    if stripped.startswith("//"):
        return True
    if stripped in ("{", "}"):
        return True
    return False


def scan_cfg_test_fn_bodies(lines: list[str], rel: str):
    """Scan for #[cfg(test)] functions and check their body line count.

    Returns two lists: (too_short, too_long) where each element is
    (rel_path, line_number, fn_name, body_line_count).
    """
    too_short = []
    too_long = []
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        # Look for #[cfg(test)]
        if not CFG_TEST_FN_HEADER_PATTERN.match(line):
            idx += 1
            continue

        cfg_test_line = idx
        idx += 1

        # Skip additional attributes (#[...])
        while idx < len(lines) and lines[idx].strip().startswith("#"):
            idx += 1

        if idx >= len(lines):
            break

        # Check if next non-attribute line is a fn declaration
        fn_match = FN_HEADER_PATTERN.match(lines[idx])
        if not fn_match:
            continue

        fn_name = fn_match.group("name")
        fn_line = idx

        # Find opening brace
        brace_depth = 0
        # The fn header may or may not contain the opening brace
        while idx < len(lines):
            brace_depth += lines[idx].count("{") - lines[idx].count("}")
            if brace_depth > 0:
                break
            idx += 1

        if idx >= len(lines) or brace_depth == 0:
            continue

        # Now scan the body (everything after the opening line until braces balance)
        idx += 1
        body_lines = 0
        while idx < len(lines) and brace_depth > 0:
            body_line = lines[idx]
            brace_depth += body_line.count("{") - body_line.count("}")
            if brace_depth > 0 and not is_trivial_line(body_line):
                body_lines += 1
            idx += 1

        # Report line number as 1-based at the #[cfg(test)] attribute
        report_line = cfg_test_line + 1
        if body_lines < 2:
            too_short.append((rel, report_line, fn_name, body_lines))
        elif body_lines > 2:
            too_long.append((rel, report_line, fn_name, body_lines))
        # body_lines == 2 is OK

    return too_short, too_long


def main() -> int:
    violations = []
    cfg_test_too_short = []
    cfg_test_too_long = []
    public_field_derive_violations = []
    dead_code_attr_violations = []
    unused_attr_violations = []

    for path in candidate_files():
        rel = path.relative_to(ROOT).as_posix()
        if rel in EXCLUDE_PATHS:
            continue
        if is_skipped(path):
            continue

        text = path.read_text(encoding="utf-8")
        for match in PUBLIC_CFG_TEST_FN_PATTERN.finditer(text):
            line = line_of_offset(text, match.start())
            violations.append((path, line, match.group("name")))

        lines = text.splitlines()

        # Scan #[cfg(test)] function body lengths
        short, long = scan_cfg_test_fn_bodies(lines, rel)
        cfg_test_too_short.extend(short)
        cfg_test_too_long.extend(long)

        # Scan struct derives (public structs with public fields only)
        pending_derive_has_api_trait = False
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            derive_match = DERIVE_PATTERN.match(line)
            if derive_match:
                derive_items = {item.strip() for item in derive_match.group("items").split(",")}
                pending_derive_has_api_trait = bool(API_TRAITS & derive_items)
                idx += 1
                continue

            if line.strip().startswith("#"):
                # Skip multi-line attributes (e.g., #[serde(bound(\n...\n))])
                bracket_depth = line.count("[") - line.count("]")
                idx += 1
                while idx < len(lines) and bracket_depth > 0:
                    bracket_depth += lines[idx].count("[") - lines[idx].count("]")
                    idx += 1
                continue

            header_match = STRUCT_HEADER_PATTERN.match(line)
            if header_match:
                struct_name = header_match.group("name")
                struct_line = idx + 1
                brace_depth = line.count("{") - line.count("}")
                has_public_field = False
                idx += 1
                while idx < len(lines) and brace_depth > 0:
                    body_line = lines[idx]
                    if PUBLIC_FIELD_PATTERN.match(body_line):
                        has_public_field = True
                    brace_depth += body_line.count("{") - body_line.count("}")
                    idx += 1

                exempt_key = f"{rel}:{struct_name}"
                if has_public_field and not pending_derive_has_api_trait and exempt_key not in EXEMPT_STRUCTS:
                    public_field_derive_violations.append((rel, struct_line, struct_name))

                pending_derive_has_api_trait = False
                continue

            if line.strip():
                pending_derive_has_api_trait = False

            idx += 1

        for line_no, line in enumerate(text.splitlines(), start=1):
            if "#[allow(dead_code)]" in line:
                dead_code_attr_violations.append((rel, line_no))

            if "#[allow(" in line and "unused" in line:
                key = f"{rel}:{line_no}"
                if key not in ALLOWED_UNUSED_ATTRS:
                    unused_attr_violations.append((rel, line_no, line.strip()))

    has_errors = False

    if violations:
        has_errors = True
        print("Disallowed #[cfg(test)] helper functions found in implementation source:\n")
        for path, line, name in violations:
            rel = path.relative_to(ROOT)
            print(f"  {rel}:{line}  fn {name}")
        print(
            "\nMove test-only functions into #[cfg(test)] mod tests { } blocks or"
            "\ninto tests/ files. If the function provides real behavior needed by"
            "\nproduction code, remove #[cfg(test)] and make it a proper method."
        )

    if cfg_test_too_short:
        has_errors = True
        print("\n#[cfg(test)] functions with bodies too short (1 line or less):\n")
        for rel, line_no, name, count in cfg_test_too_short:
            print(f"  {rel}:{line_no}  fn {name}  ({count} line)")
        print(
            "\nToo trivial for a #[cfg(test)] helper."
            "\nTest through the type's public API (e.g., prepare/commit/do_uncommitted_get)"
            "\ninstead of wrapping internal field access."
        )

    if cfg_test_too_long:
        has_errors = True
        print("\n#[cfg(test)] functions with bodies too long (3+ lines):\n")
        for rel, line_no, name, count in cfg_test_too_long:
            print(f"  {rel}:{line_no}  fn {name}  ({count} lines)")
        print(
            "\nToo complex for a #[cfg(test)] helper."
            "\nExtract this logic into a proper method or type in implementation code,"
            "\nthen test through that type's public API."
        )

    if public_field_derive_violations:
        has_errors = True
        print("\nPublic structs with public fields must derive at least one API trait:\n")
        for rel, line_no, name in public_field_derive_violations:
            print(f"  {rel}:{line_no}  struct {name}")
        print(
            "\nPublic structs with public fields must derive at least one of Clone, Eq,"
            "\nSerialize, or Deserialize. If the struct is an internal implementation"
            "\ndetail, make it non-public (remove pub). If it is a genuine API type,"
            "\nadd the appropriate derive."
        )

    if dead_code_attr_violations:
        has_errors = True
        print("\nDisallowed #[allow(dead_code)] attributes found:\n")
        for rel, line_no in dead_code_attr_violations:
            print(f"  {rel}:{line_no}")
        print(
            "\nRemove the attribute. If the code is only used in tests, gate it with"
            "\n#[cfg(test)] so the compiler enforces it is actually used in test builds."
            "\nIf the code is truly unused, delete it. Do not suppress the warning."
        )

    if unused_attr_violations:
        has_errors = True
        print("\nDisallowed new #[allow(unused*)] attributes found:\n")
        for rel, line_no, line in unused_attr_violations:
            print(f"  {rel}:{line_no}  {line}")
        print("\nRemove the attribute by deleting unused code/imports or by making the symbol genuinely used; only update baseline intentionally.")

    if has_errors:
        return 1

    print("OK: no lint violations found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
