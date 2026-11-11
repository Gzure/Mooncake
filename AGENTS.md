# AGENTS.md

## `docs/` Directory Changes

- Before modifying files under `docs/`, read `docs/AGENTS.md`.

## Pull Request Guidelines

- Follow `CONTRIBUTING.md` for PR title prefixes, RFC expectations, and
  contribution workflow.
- Before opening a PR for nontrivial work, check whether an existing issue or
  open PR already covers the same change. If the work overlaps, explain the
  difference instead of duplicating it.
- Do not open low-value busywork PRs for isolated typo, style, or mechanical
  changes unless they are part of a substantive requested change.
- Use `.github/pull_request_template.md` when preparing a PR, and fill in the
  relevant sections for description, module, type of change, testing,
  checklist, and AI assistance disclosure.
- For AI-assisted changes, make sure the human submitter has reviewed every
  changed line and can defend the change end-to-end.
- Run pre-commit locally on the files touched by the change before handoff when
  the toolchain is available. If broader hooks or `pre-commit run --all-files`
  rewrite unrelated files, do not include those unrelated edits in the PR.
- Keep PRs lean: review `git diff` before staging, and include only changes
  required for the requested task.

## graphify

A graphify knowledge graph is built at `graphify-out/`, **scoped to
`mooncake-store/`** (18,417 nodes, 34,769 edges, 667 communities, built
2026-11-03). It does not cover `mooncake-transfer-engine/`, `mooncake-common/`,
`mooncake-pg/`, or the other modules yet.

Artifacts:
- `graphify-out/graph.json` - raw graph (nodes, edges, communities)
- `graphify-out/GRAPH_REPORT.md` - audit report: god nodes, communities, cohesion
- `graphify-out/graph.html` - interactive view. It is community-aggregated
  because 18k nodes is above graphify's 5k node limit; node-level detail lives in
  `graph.json`.

When the user types `/graphify`, load the graphify skill before doing anything
else.

For questions about `mooncake-store/`, query the graph before reading source:
- `graphify query "<question>"` - scoped subgraph, broad context
- `graphify query "<question>" --dfs` - trace one specific path
- `graphify path "<A>" "<B>"` - shortest path between two concepts
- `graphify explain "<concept>"` - plain-language explanation of one node

These are much cheaper than raw browsing: a query costs ~18.5k tokens versus
~1.23M for reading the corpus naively (about 66x less).

Read `GRAPH_REPORT.md` only for broad architecture review, or when
query/path/explain do not surface enough context. Fall back to source for
questions outside the graph's scope, when the graph is demonstrably stale, or
when the user says not to use it.

After changing code under `mooncake-store/`, refresh the graph with
`graphify update` (re-extracts only changed files, AST-only, no API cost; it
reads the recorded scan root from `graphify-out/.graphify_root`).

Sandbox note: in the DSH Windows sandbox, `multiprocessing` named pipes are
denied, so graphify's parallel AST extraction fails with `PermissionError` on
`multiprocessing.Pipe`. Extract serially instead:
`graphify.extract.extract(files, cache_root=Path('.'), root=<scan root>, parallel=False)`.
