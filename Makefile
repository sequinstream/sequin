.PHONY: dev deviex signoff signoff-dirty signoff_stack merge help init spellcheck addword check-links deploy buildpush buildpush-dirty remiex connectdb connect

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

release-dirty:
	@./scripts/release.sh --dirty

release-gh:
	@./scripts/release.sh --github-actions

release-dirty-gh:
	@./scripts/release.sh --dirty --github-actions

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

check-links:
	@cd docs && mintlify broken-links

buildpush:
	mix buildpush

buildpush-dirty:
	mix buildpush --dirty

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
	@echo "  make check-links - Run mintlify broken-links in the docs directory"
	@echo "  make deploy [sha=<commit-sha>] - Deploy the specified or latest commit"
	@echo "  make buildpush - Run mix buildpush (build and push docker image)"
	@echo "  make buildpush-dirty - Run mix buildpush with --dirty flag"
	@echo "  make connectdb [id=<id>] [open=<open>] - Connect to the production database"
	@echo "  make connect - Connect to the production database"
	@echo "  make release     - Run the release script"
	@echo "  make release-dirty - Run the release script with --dirty flag"
	@echo "  make release-gh  - Run the release script using GitHub Actions for Docker builds"
	@echo "  make release-dirty-gh - Run the release script with --dirty flag using GitHub Actions"

impersonate:
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_rpc.sh "Sequin.Accounts.Impersonate.generate_link\(~s{$(impersonating_user_id)}\,~s{$(impersonated_user_id)}\)"

deploy:
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/deploy.sh $(sha)

remiex:
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_sequin.sh remote

connectdb:
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_db.sh $(id) $(open)

connect:
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_ssh.sh

%:
	@:
