#!/usr/bin/env python3

import argparse
import pathlib
import re
import sys


def replace_once(content: str, pattern: re.Pattern[str], replacement: str, label: str) -> str:
    updated, count = pattern.subn(replacement, content, count=1)
    if count != 1:
        raise ValueError(f"Expected to update exactly one {label}, updated {count}")
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Update Homebrew Proton formula metadata.")
    parser.add_argument("--formula", required=True, help="Path to Formula/proton.rb")
    parser.add_argument("--version", required=True, help="Release version without the leading v")
    parser.add_argument("--arm-sha256", required=True, help="SHA256 for Darwin arm64 tarball")
    parser.add_argument("--x86-sha256", required=True, help="SHA256 for Darwin x86_64 tarball")
    args = parser.parse_args()

    formula_path = pathlib.Path(args.formula)
    content = formula_path.read_text()

    content = replace_once(
        content,
        re.compile(r'(?m)^(\s*version\s+)"[^"]+"$'),
        rf'\1"{args.version}"',
        "formula version",
    )
    content = replace_once(
        content,
        re.compile(r'(?ms)(if Hardware::CPU\.arm\?\n\s*url\s+"[^"]+"\n\s*sha256\s+)"[^"]+"'),
        rf'\1"{args.arm_sha256}"',
        "arm64 sha256",
    )
    content = replace_once(
        content,
        re.compile(r'(?ms)(else\n\s*url\s+"[^"]+"\n\s*sha256\s+)"[^"]+"'),
        rf'\1"{args.x86_sha256}"',
        "x86_64 sha256",
    )

    formula_path.write_text(content)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Failed to update formula: {exc}", file=sys.stderr)
        raise SystemExit(1)
