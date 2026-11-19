# Contributing to miniKV

Thank you for your interest in contributing.

## Reporting Issues

Open a GitHub issue with:
- A short description of the problem
- Steps to reproduce
- Expected and actual behavior
- Go version and OS

## Submitting Pull Requests

1. Fork the repository and create a branch from `main`.
2. Write tests for any new behavior. Coverage targets:
   - `wal`, `replication`, `gossip`: >= 85%
   - `store`, `lru`, `ring`: >= 90%
3. Run the full gate before opening a PR:

   ```
   make fmt
   make vet
   make race
   ```

4. Use conventional commit messages (`feat`, `fix`, `test`, `docs`, `ci`, `chore`).
   No emojis in commit messages, code, or documentation.
5. Keep each logical change in its own commit. Do not batch unrelated fixes.
6. Update `docs/design.md` and `README.md` if your change affects architecture,
   configuration options, or the public API.

## Coding Conventions

- Standard Go formatting enforced by `gofmt`. Run `make fmt` before committing.
- All exported types and functions must have a doc comment.
- Use `context.Context` for cancellation; propagate it through call chains.
- Prefer `log/slog` for structured logging. No bare `fmt.Println` in production paths.
- Sharded locks over a single global mutex; document the locking order if more
  than one mutex is held at once.
- No external runtime dependencies beyond those already in `go.mod`.

## Running Tests

```
# Unit tests with race detector
make race

# All benchmarks
make bench

# Local cluster (requires Bash on Linux/macOS or Git Bash on Windows)
make run-cluster

# Docker cluster
make docker-up
```

## License

By contributing, you agree that your contributions will be licensed under the
MIT License that covers this project.
