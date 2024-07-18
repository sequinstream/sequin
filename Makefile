.PHONY: build release dev deviex cli build-cli signoff signoff_stack merge help

dev:
	@$(MAKE) -C server dev

deviex:
	@$(MAKE) -C server deviex

build-cli:
	@$(MAKE) -C cli build

cli:
	@cd cli && go run main.go $(filter-out $@,$(MAKECMDGOALS))

signoff:
	@./scripts/signoff.sh

signoff_stack:
	@./scripts/signoff_stack.sh

merge:
	@./scripts/merge.sh

help:
	@echo "Available commands:"
	@echo "  make build     - Build and install the sequin binary (delegated to cli)"
	@echo "  make release   - Run the release process (delegated to cli)"
	@echo "  make dev       - Run the app locally (delegated to server)"
	@echo "  make deviex    - Open an IEx session on the running local app (delegated to server)"
	@echo "  make build-cli - Build the CLI (delegated to cli)"
	@echo "  make signoff   - Run the signoff script"
	@echo "  make signoff_stack - Run the signoff_stack script"
	@echo "  make merge     - Run the merge script"
	@echo "  make cli       - Run 'go run main.go' in the cli/ directory"
	@echo "  make help      - Show this help message"

%:
	@: