# Agent instructions

When working in this repository, run the following commands to verify your changes.

If you have access to a setup Postgres and Redis, you can additionally run `mix test`. (If the first run fails, you can run `mix test --failed`.)

```bash
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

These commands should pass before committing.