# Contributing

Thanks for your interest in contributing! This repository is a community
guide — the bar is **correct, teachable, and runnable**.

## Ways to contribute

- **Corrections** — if a pattern description is wrong or misleading, open an
  issue with the evidence (or a PR with the fix).
- **New examples** — variants of `examples/outbox-inbox` using Kafka, RabbitMQ,
  Watermill, or a CDC-based relay are especially welcome. Follow the same
  shape: single runnable module, `docker-compose.yml`, own README with
  run/inspect/cleanup instructions, ports offset from common local services.
- **Pattern write-ups** — real production war stories (what burned you, what
  fixed it) make this guide better than theory does.
- **Translations** — translated READMEs live as `README.<locale>.md` and are
  linked from the main README.

## Ground rules for code

- Go code must pass `gofmt`, `go vet`, and `go build` (CI enforces this).
- Examples must actually run end-to-end — test them with
  `docker compose up -d && go run .` before opening the PR, and paste the
  relevant output in the PR description.
- Keep dependencies minimal and justify each new one.
- Prefer small, commented, single-purpose files over clever abstractions —
  this is teaching material.

## Commits and PRs

- Use [Conventional Commits](https://www.conventionalcommits.org/):
  `docs: ...`, `feat: ...`, `fix: ...`, `style: ...`.
- One concern per PR. Reference the issue if one exists.

## Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Be kind;
assume good faith; teach, don't dunk.
