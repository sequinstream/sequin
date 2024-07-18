.PHONY: build release dev deviex cli build-cli signoff signoff-dirty signoff_stack merge help init

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

signoff-dirty:
	@./scripts/signoff.sh --dirty

signoff_stack:
	@./scripts/signoff_stack.sh

merge:
	@./scripts/merge.sh

init:
	@if [ ! -f .settings.json ]; then \
		cp .settings.example.json .settings.json; \
		echo ".settings.json created. Please edit it to configure your settings."; \
	else \
		echo ".settings.json already exists. No changes made."; \
	fi

help:
	@echo "Available commands:"
	@echo "  make build     - Build and install the sequin binary (delegated to cli)"
	@echo "  make release   - Run the release process (delegated to cli)"
	@echo "  make dev       - Run the app locally (delegated to server)"
	@echo "  make deviex    - Open an IEx session on the running local app (delegated to server)"
	@echo "  make build-cli - Build the CLI (delegated to cli)"
	@echo "  make signoff   - Run the signoff script"
	@echo "  make signoff-dirty - Run the signoff script with --dirty flag"
	@echo "  make signoff_stack - Run the signoff_stack script"
	@echo "  make merge     - Run the merge script"
	@echo "  make cli       - Run 'go run main.go' in the cli/ directory"
	@echo "  make init      - Create .settings.json from .settings.json.example"
	@echo "  make help      - Show this help message"

%:
	@: