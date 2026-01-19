# Development Practices

## Code style (C++)
- Use modern C++ style (C++20+) consistent with the codebase/toolchain.
- `/// proton: starts` / `/// proton: ends` usage:
	- Wrap new or modified code to distinguish local changes from existing code.
	- Do **not** add new fences inside the Streaming directory or Streaming namespace, and do not nest inside regions already fenced with proton markers.
	- The existing codebase outside streaming is already wrapped where needed—reuse those fences when extending nearby code.
	- Use fences when touching third-party synced areas (upstream/vendor code we mirror) or community/shared sections where local deltas must stay explicit.
	- Use fences in any block where reviewers must see local patches separated from upstream.
- Function names: lowerCamelCase.
- Variable names: lowercase_with_underscores.
- Run clang-format on the code you change; avoid reformatting entire files unless necessary.
- Use `///` for comments; keep them minimal and purposeful, and avoid restating the obvious.

## SQL style (summary)
- Keywords uppercase; functions/types/identifiers lowercase_with_underscores.
- Streaming-first: use `CREATE STREAM`, and EMIT/window syntax per docs.
- See [CONVENTIONS_SQL.md](CONVENTIONS_SQL.md) for details and examples.

## Tests before PRs
- Targeted gtests when you touch parsers/interpreters: `./build/src/stripped/bin/unit_tests_dbms --gtest_filter="<Suite>*"`.
- Relevant stateless cases when modifying SQL behavior: `cd tests && ./ported-clickhouse-test.py -b ../build/programs/stripped/bin/timeplusd -q queries_ported <case>`.

## V8 PKU (Linux x86)
- PKRU is per-thread; on Linux x86 with PKU enabled, threads may enter V8 with a “wrong” PKRU and crash with `SEGV_PKUERR`.
- Any direct V8 API entry must be guarded with `DB::V8::PkuSupport::ThreadPkruGuard` (especially `v8::Isolate::New()` / `v8::Isolate::Dispose()`).
- Prefer using `DB::V8::run()` / `DB::V8::compileSource()` helpers when possible (they already guard V8 entry).

## Adding/Updating conventions
- Place new coding conventions here if they are not SQL-specific.
- If a rule belongs to build/test or SQL, update the respective file and link from [CONVENTIONS.md](CONVENTIONS.md).
