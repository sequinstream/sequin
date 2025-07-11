# Contributing to Sequin

Thank you for your interest in contributing to Sequin! This document provides guidelines for contributing to the project.

## Development setup

### Prerequisites

- Elixir 1.18+
- PostgreSQL 14+
- GitHub CLI (`gh`)
- Node.js (for frontend assets)
- Go (only necessary for CLI development)

### Getting started

1. Fork and clone the repository
2. Start PostgreSQL and Redis with docker compose
    ```bash
    docker compose --profile databases up -d
    ```

    If you encounter an error about the default PostgreSQL user "postgres" not existing, you can create the user with:
    ```bash
    createuser -s postgres
    ```

3. Install Elixir, Erlang, and Node.js through `asdf`
    Install and configure `asdf` if you don't have it already: https://asdf-vm.com/guide/getting-started.html

    On Mac, you can use Homebrew:
    ```bash
    brew install asdf
    ```

    Add the following to your shell configuration file (e.g., `.zshrc` or `.bashrc`) to configure `asdf`:
    ```bash
    . /opt/homebrew/opt/asdf/libexec/asdf.sh
    ```

    Now you can use `asdf` to install the necessary versions of Elixir, Erlang, and Node.js:
    ```bash
    asdf plugin add erlang
    asdf plugin add elixir
    asdf plugin add nodejs
    ```

    Run this in the root of the repository to use our `.tool-versions` file:
    ```bash
    asdf install
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

#### For external contributors

When you submit a pull request, GitHub Actions will automatically run a series of checks.

After your PR passes, we will review your changes and hopefully merge!

If you need to reach us sooner, the fastest way is through our [Discord server](https://discord.gg/BV8wFXvNtY) or [Slack community](https://join.slack.com/t/sequin-community/shared_invite/zt-37begzach-4aUwR5xt_XgivdvctZDemA).

#### For core contributors

Core contributors with repository access should run the signoff script before merging:

```bash
# From the project root
./scripts/signoff.sh
```

This script:
1. Checks code formatting
2. Performs compilation with warnings as errors
3. Runs all tests
4. Performs linting and other checks
5. Updates the GitHub commit status with a signoff

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

- Join our [Discord server](https://discord.gg/BV8wFXvNtY) or [Slack community](https://join.slack.com/t/sequin-community/shared_invite/zt-37begzach-4aUwR5xt_XgivdvctZDemA) for questions and discussions
- Open an issue for bugs or feature requests
- Tag maintainers in your PR if you need help

## License

By contributing to Sequin, you agree that your contributions will be licensed under the MIT License.
