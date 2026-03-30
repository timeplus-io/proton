#!/usr/bin/env python3
"""Summarize collapsed allocation profiles for Proton/ClickHouse-style stacks."""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path

NOISE_PREFIXES = (
    "prof_backtrace",
    "prof_alloc_prep",
    "prof_tctx",
    "prof_",
    "imalloc",
    "ialloc",
    "irallocx",
    "imallocx",
    "arena_",
    "tcache_",
    "large_",
    "chunk_alloc",
    "huge_",
    "je_malloc",
    "je_calloc",
    "je_realloc",
    "je_rallocx",
    "je_mallocx",
    "je_posix_memalign",
    "je_aligned_alloc",
    "malloc_default",
    "calloc",
)

NOISE_SUBSTRINGS = (
    "operator new",
    "__libcpp_operator_new",
    "__libc_malloc",
    "__libc_calloc",
    "_int_malloc",
    "posix_memalign",
    "aligned_alloc",
    "do_rallocx",
    "do_mallocx",
    "mi_malloc",
    "mi_calloc",
    "DB::Memory<",
    "Memory::newImpl",
    "Allocator<false",
    "Allocator<true",
    "allocNoTrack",
    "PODArrayBase::realloc",
    "PODArrayBase::alloc",
    "CRYPTO_malloc",
    "std::__detail::_Hash_node",
    "std::_Rb_tree",
    "std::vector<",
    "std::string::",
    "std::__1::",
    "DB::PODArrayBase",
)

SKIP_OUTER = (
    "0000",
    "_start",
    "__libc_start",
    "__GI___clone",
    "start_thread",
    "clone3",
    "ThreadPoolImpl",
    "ThreadFromGlobalPool",
    "std::__1::__function",
    "std::__1::__invoke",
    "decltype",
    "void std::__1::__function",
    "std::__1::__packaged_task_function",
    "DB::ThreadPool",
    "DB::GlobalThreadPool",
    "DB::threadFunction",
    "BaseDaemon",
    "SignalListener",
    "Poco::ThreadImpl::runnableEntry",
    "Poco::PooledThread::run",
    "main",
    "DB::Server::run",
    "Poco::Util::Application::run",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("profile", type=Path)
    parser.add_argument("--top", type=int, default=25)
    return parser.parse_args()


def shorten(frame: str) -> str:
    frame = re.sub(r"<[^>]{40,}>", "<...>", frame)
    frame = frame.replace("(anonymous namespace)", "{anon}")
    frame = re.sub(r"\(.*", "", frame)
    return frame.replace("{anon}", "(anonymous namespace)")[:140]


def is_noise(frame: str) -> bool:
    return any(frame.startswith(prefix) for prefix in NOISE_PREFIXES) or any(
        needle in frame for needle in NOISE_SUBSTRINGS
    )


def is_skip_outer(frame: str) -> bool:
    return frame.startswith("(") or any(frame.startswith(prefix) for prefix in SKIP_OUTER)


def parse_profile(path: Path) -> list[tuple[int, str]]:
    traces: list[tuple[int, str]] = []
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        try:
            traces.append((int(parts[1]), parts[0]))
        except ValueError:
            continue
    return traces


def main() -> int:
    args = parse_args()
    traces = parse_profile(args.profile)
    if not traces:
        raise SystemExit(f"no collapsed traces found in {args.profile}")

    total = sum(value for value, _ in traces)
    traces.sort(reverse=True)

    by_outer: defaultdict[str, int] = defaultdict(int)
    by_leaf: defaultdict[str, int] = defaultdict(int)

    print("=== SUMMARY ===")
    print(f"File: {args.profile}")
    print(f"Total allocated: {total:,} bytes ({total / 1024 / 1024:.2f} MiB)")
    print(f"Unique stack traces: {len(traces)}")
    print()

    for value, stack in traces:
        frames = [frame for frame in stack.split(";") if frame]
        if not frames:
            continue

        meaningful = [frame for frame in frames if not is_noise(frame)]
        leaf = meaningful[-1] if meaningful else frames[-1]
        by_leaf[shorten(leaf)] += value

        outer = next((frame for frame in frames if frame and not is_skip_outer(frame)), frames[0])
        by_outer[shorten(outer)] += value

    print(f"=== TOP {args.top} STACK TRACES ===")
    for index, (value, stack) in enumerate(traces[: args.top], start=1):
        frames = [frame for frame in stack.split(";") if frame]
        meaningful = [frame for frame in frames if not is_noise(frame)]
        tail_frames = meaningful[-4:] if meaningful else frames[-4:]
        tail = " <- ".join(shorten(frame) for frame in reversed(tail_frames))
        print(
            f"{index:>3}. {value / 1024 / 1024:>8.2f} MiB "
            f"({100 * value / total:>5.1f}%) {tail}"
        )

    print()
    print(f"=== TOP {args.top} OUTERMOST MEANINGFUL FRAMES ===")
    for frame, value in sorted(by_outer.items(), key=lambda item: -item[1])[: args.top]:
        print(f"{value / 1024 / 1024:>10.2f} MiB ({100 * value / total:>5.1f}%) {frame}")

    print()
    print(f"=== TOP {args.top} LEAF ALLOCATION FRAMES ===")
    for frame, value in sorted(by_leaf.items(), key=lambda item: -item[1])[: args.top]:
        print(f"{value / 1024 / 1024:>10.2f} MiB ({100 * value / total:>5.1f}%) {frame}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
