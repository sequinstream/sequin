.PHONY: build release dev deviex cli build-cli

dev:
	@$(MAKE) -C server dev

deviex:
	@$(MAKE) -C server deviex

build-cli:
	@$(MAKE) -C cli build

cli:
	@cd cli && go run main.go $(filter-out $@,$(MAKECMDGOALS))

signoff:
	@cd server && mix signoff

signoff_stack:
	@cd server && mix signoff_stack

help:
	@echo "Available commands:"
	@echo "  make build     - Build and install the sequin binary (delegated to cli)"
	@echo "  make release   - Run the release process (delegated to cli)"
	@echo "  make dev       - Run the app locally (delegated to server)"
	@echo "  make deviex    - Open an IEx session on the running local app (delegated to server)"
	@echo "  make build-cli - Build the CLI (delegated to cli)"
	@echo "  make signoff        - Run 'mix signoff' in the server directory"
	@echo "  make signoff_stack  - Run 'mix signoff_stack' in the server directory"
	@echo "  make cli       - Run 'go run main.go' in the cli/ directory"
	@echo "  make help      - Show this help message"

%:
	@: