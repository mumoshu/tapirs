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

# Detects test-gated one-line wrappers that only delegate to an internal
# `self.<...>(...)` call. This targets call-through helpers that expose
# internals for tests without owning behavior.
# Does NOT attempt full Rust parsing; multi-statement bodies are intentionally
# not matched here.
CFG_TEST_DELEGATING_WRAPPER_PATTERN = re.compile(
    r"""(?mxs)
    ^[ \t]*\#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\][ \t]*\n
    (?:^[ \t]*\#\s*\[[^\]]+\][ \t]*\n)*
    ^[ \t]*(?:pub(?:\([^)]+\))?[ \t]+)?fn[ \t]+(?P<name>[A-Za-z_]\w*)[^{]*\{[ \t]*\n
    [ \t]*(?:return[ \t]+)?self(?:\.[A-Za-z_]\w*)+\([^;{}]*\)[ \t]*;?[ \t]*\n
    [ \t]*\}
    """
)

# Detects public one-line delegating wrappers that simply forward to an
# internal `self.<...>(...)` call. This enforces “no thin public call-through
# wrappers” as an encapsulation guardrail.
# Does NOT match non-public functions.
PUBLIC_DELEGATING_WRAPPER_PATTERN = re.compile(
    r"""(?mxs)
    ^[ \t]*(?:\#\s*\[[^\]]+\][ \t]*\n)*
    ^[ \t]*pub(?:\([^)]+\))?[ \t]+fn[ \t]+(?P<name>[A-Za-z_]\w*)[^{]*\{[ \t]*\n
    [ \t]*(?:return[ \t]+)?self(?:\.[A-Za-z_]\w*)+\([^;{}]*\)[ \t]*;?[ \t]*\n
    [ \t]*\}
    """
)

# Detects a struct header line that opens a braced struct body.
# Used as the anchor for scanning struct fields and associated derives.
# Does NOT match tuple structs or unit structs.
STRUCT_HEADER_PATTERN = re.compile(
    r"""(?mx)
    ^[ \t]*(?:pub(?:\([^)]+\))?[ \t]+)?struct[ \t]+(?P<name>[A-Za-z_]\w*)\b[^{;]*\{[ \t]*$
    """
)

# Detects a `#[derive(...)]` line and captures derive items.
# Used to identify whether `Eq` is present for nearby struct declarations.
DERIVE_EQ_PATTERN = re.compile(r"^\s*#\s*\[\s*derive\s*\((?P<items>[^)]*)\)\s*\]\s*$")

# Detects a field line starting with a Rust visibility modifier (`pub`,
# `pub(crate)`, `pub(super)`, etc.). Used to identify structs with public
# fields for the Eq-based API-object heuristic.
PUBLIC_FIELD_PATTERN = re.compile(r"^\s*pub(?:\([^)]+\))?\s+")


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


def main() -> int:
    violations = []
    delegating_wrapper_violations = []
    public_delegating_wrapper_violations = []
    public_field_eq_violations = []
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

        for match in CFG_TEST_DELEGATING_WRAPPER_PATTERN.finditer(text):
            line = line_of_offset(text, match.start())
            delegating_wrapper_violations.append((rel, line, match.group("name")))

        for match in PUBLIC_DELEGATING_WRAPPER_PATTERN.finditer(text):
            line = line_of_offset(text, match.start())
            public_delegating_wrapper_violations.append((rel, line, match.group("name")))

        lines = text.splitlines()
        pending_derive_has_eq = False
        idx = 0
        while idx < len(lines):
            line = lines[idx]
            derive_match = DERIVE_EQ_PATTERN.match(line)
            if derive_match:
                derive_items = [item.strip() for item in derive_match.group("items").split(",")]
                pending_derive_has_eq = any(item == "Eq" for item in derive_items)
                idx += 1
                continue

            if line.strip().startswith("#"):
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

                if has_public_field and not pending_derive_has_eq:
                    public_field_eq_violations.append((rel, struct_line, struct_name))

                pending_derive_has_eq = False
                continue

            if line.strip():
                pending_derive_has_eq = False

            idx += 1

        for line_no, line in enumerate(text.splitlines(), start=1):
            if "#[allow(dead_code)]" in line:
                dead_code_attr_violations.append((rel, line_no))

            if "#[allow(" in line and "unused" in line:
                key = f"{rel}:{line_no}"
                if key not in ALLOWED_UNUSED_ATTRS:
                    unused_attr_violations.append((rel, line_no, line.strip()))

    if violations:
        print("Disallowed #[cfg(test)] helper functions found in implementation source:\n")
        for path, line, name in violations:
            rel = path.relative_to(ROOT)
            print(f"  {rel}:{line}  fn {name}")
        print("\nMove these into #[cfg(test)] mod tests or tests/ files, or make it a real method on a relevant struct.")
        if (
            not delegating_wrapper_violations
            and not public_delegating_wrapper_violations
            and not public_field_eq_violations
            and not dead_code_attr_violations
            and not unused_attr_violations
        ):
            return 1

    if delegating_wrapper_violations:
        print("Disallowed #[cfg(test)] one-line delegating wrappers found:\n")
        for rel, line_no, name in delegating_wrapper_violations:
            print(f"  {rel}:{line_no}  fn {name}")
        print("\nDo not expose internals via call-through wrappers. Test behavior through real APIs or add a meaningful method with owned logic.")
        if (
            not public_delegating_wrapper_violations
            and not public_field_eq_violations
            and not dead_code_attr_violations
            and not unused_attr_violations
        ):
            return 1

    if public_delegating_wrapper_violations:
        print("Disallowed public one-line delegating wrappers found:\n")
        for rel, line_no, name in public_delegating_wrapper_violations:
            print(f"  {rel}:{line_no}  fn {name}")
        print("\nAvoid public call-through wrappers over internals. Keep behavior cohesive in the owning type.")
        if not public_field_eq_violations and not dead_code_attr_violations and not unused_attr_violations:
            return 1

    if public_field_eq_violations:
        print("Structs with public fields must derive Eq (temporary API-object heuristic):\n")
        for rel, line_no, name in public_field_eq_violations:
            print(f"  {rel}:{line_no}  struct {name}")
        print("\nEither add #[derive(Eq)] or make fields non-public and expose behavior through methods.")
        if not dead_code_attr_violations and not unused_attr_violations:
            return 1

    if dead_code_attr_violations:
        print("Disallowed #[allow(dead_code)] attributes found:\n")
        for rel, line_no in dead_code_attr_violations:
            print(f"  {rel}:{line_no}")
        print("\nRemove the attribute. Prefer deleting unused code, making code paths genuinely used, or moving test-only code into test modules.")
        return 1

    if unused_attr_violations:
        print("Disallowed new #[allow(unused*)] attributes found:\n")
        for rel, line_no, line in unused_attr_violations:
            print(f"  {rel}:{line_no}  {line}")
        print("\nRemove the attribute by deleting unused code/imports or by making the symbol genuinely used; only update baseline intentionally.")
        return 1

    print("OK: no #[cfg(test)] helper functions in implementation source.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
