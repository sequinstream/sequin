# Agent instructions

When working in this repository, run the following commands to verify your changes.

```bash
# Test whole repo
mix test

# Test individual test you worked on
mix test test/sequin/runtime/slot_producer/slot_producer_test.exs:1234

# Test individual file you worked on:
mix test test/sequin/runtime/slot_producer/slot_producer_test.exs

# Re-run tests on intermittent failure
mix test --failed

mix format --check-formatted
MIX_ENV=prod mix compile --warnings-as-errors

# CLI checks
cd cli
go test ./cli
if unformatted=$(gofmt -s -l .); [ -n "$unformatted" ]; then
  echo "Go files need formatting: $unformatted" && exit 1
fi
go vet ./...
go build -o /dev/null ./...
cd ..

# Spelling and docs
make spellcheck
make check-links

# Frontend assets
cd assets
npm run format:check
npm run tsc
cd ..
```

These commands should pass before committing. Warnings or noise in test is not acceptable.

## Using jj workspaces for isolated work

When working on a feature or fix, you can create an isolated workspace using jj:

```bash
# Create a new workspace (from the main repo)
jj workspace add ../sequin-feature-name

# The new workspace needs deps and _build symlinked from the main repo
cd ../sequin-feature-name
ln -s ../sequin/deps .
ln -s ../sequin/_build .

# Now you can run tests and make changes in this isolated workspace
mix test test/path/to/test.exs
```

## Tidewave

Always use Tidewave's tools for evaluating code, querying the database, etc.

Use `get_docs` to access documentation and the `get_source_location` tool to
find module/function definitions.