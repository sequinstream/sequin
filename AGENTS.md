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
