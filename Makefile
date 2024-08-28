.PHONY: dev deviex signoff signoff-dirty signoff_stack merge help init spellcheck addword

dev: ## Run the app locally
	elixir --sname sequin-stream-dev --cookie sequin-stream-dev -S mix phx.server

deviex: ## Open an IEx session on the running local app
	iex --sname console-$$(openssl rand -hex 4) --remsh sequin-stream-dev --cookie sequin-stream-dev

signoff:
	@./scripts/signoff.sh

signoff-dirty:
	@./scripts/signoff.sh --dirty

signoff_stack:
	@./scripts/signoff_stack.sh

merge:
	@./scripts/merge.sh

merge-force:
	@./scripts/merge.sh --force

init:
	@if [ ! -f .settings.json ]; then \
		cp .settings.example.json .settings.json; \
		echo ".settings.json created. Please edit it to configure your settings."; \
	else \
		echo ".settings.json already exists. No changes made."; \
	fi

release:
	@./scripts/release.sh

spellcheck:
	@npx -y cspell "**/*.{md,mdx}" --config spellcheck/.cspell.json

addword:
	@if [ -z "$(word)" ]; then \
		echo "Usage: make addword word=<word>"; \
	else \
		echo "$(word)" >> spellcheck/project-words.txt; \
		sort -u spellcheck/project-words.txt -o spellcheck/project-words.txt; \
		echo "Added '$(word)' to project-words.txt"; \
	fi

help:
	@echo "Available commands:"
	@echo "  make dev       - Run the app locally"
	@echo "  make deviex    - Open an IEx session on the running local app"
	@echo "  make signoff   - Run the signoff script"
	@echo "  make signoff-dirty - Run the signoff script with --dirty flag"
	@echo "  make signoff_stack - Run the signoff_stack script"
	@echo "  make merge     - Run the merge script"
	@echo "  make merge-force - Run the merge script, bypassing signoff check"
	@echo "  make init      - Create .settings.json from .settings.json.example"
	@echo "  make help      - Show this help message"
	@echo "  make spellcheck - Run cspell to check spelling in .md and .mdx files"
	@echo "  make addword word=<word> - Add a word to project-words.txt"

%:
	@: