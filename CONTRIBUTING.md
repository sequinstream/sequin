# Contributing to Sequin

Thank you for your interest in contributing to Sequin! This document outlines the process for contributing and getting your changes merged.

## Development setup

### Prerequisites

- Elixir 1.18+
- PostgreSQL 14+
- GitHub CLI (`gh`)
- Node.js (for frontend assets)
- Go (only necessary for CLI development)

### Getting started

1. Fork and clone the repository
2. Install dependencies:
   ```bash
   # Install Elixir dependencies
   mix deps.get
   
   # Install frontend dependencies
   npm install --prefix assets
   ```
3. Start PostgreSQL and Redis with docker compose
    ```bash
    docker compose up -d
    ```
4. Run the setup script
    ```bash
    mix setup
    ```
5. Start the development server:
   ```bash
   make dev
   ```

The app will be available at `http://localhost:4000`.

Sequin uses LiveView + LiveSvelte for its frontend. As a monolith, the entire app is available at `http://localhost:4000`.

## Development workflow

1. Create a new branch for your changes
2. Make your changes
3. Run tests:
   ```bash
   # Run Elixir tests
   mix test
   ```
4. Push your changes and open a pull request
5. Run `make signoff` to verify your changes pass all checks

### Signoff process

Before merging, all PRs must be signed off using `make signoff`. This runs:

- Code formatting checks
- Compilation with warnings as errors
- Test suite
- Spell checking
- Link checking
- TypeScript checks
- Frontend formatting checks

If you can't run the signoff locally, mention this in your PR and a maintainer can run it for you.

## Code style

- Elixir: Follow the standard Elixir formatting (enforced by `mix format`)
- TypeScript/JavaScript: Use Prettier (enforced by the signoff process)
- Go: Use standard Go formatting (enforced by `go fmt`)
- SQL: Use lowercase for keywords

## Documentation

- Update relevant documentation in the `docs/` directory
- Follow the [style guide](./STYLE.md) for documentation
- Run the docs locally using `make docs`

## Getting help

- Join our [Discord server](https://discord.gg/BV8wFXvNtY) for questions and discussions
- Open an issue for bugs or feature requests
- Tag maintainers in your PR if you need help

## License

By contributing to Sequin, you agree that your contributions will be licensed under the MIT License.
