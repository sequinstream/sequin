# Running `make` will trigger `make help`
.DEFAULT_GOAL := help

# for formatting
BOLD="\033[1m"
NORMAL="\033[0m"

# Any target invoked by make target does not correspond to a file, so
# it should always be executed. This avoids: 1. Having to remember to
# add newly created targets as PHONY.  2. Using a catch-all target
# (%:) with a no-op (@:) to prevent failures for non-existing targets.
.PHONY: $(MAKECMDGOALS)

help: ## Prints target: [dep1 dep1 ...]  and what it does
	@echo -e ${BOLD}Available targets:${NORMAL}
	@grep -E '^[a-zA-Z_-]+.*## .*$$' $(MAKEFILE_LIST) |  sed 's/^Makefile://' | column -t -s"##"

dev: ## Run the app locally
	iex --name sequin-stream-dev@127.0.0.1 --cookie sequin-stream-dev -S mix phx.server

dev2: ## Run a second node locally
	SERVER_PORT=4001 elixir --name sequin-stream-dev2@127.0.0.1 --cookie sequin-stream-dev -S mix phx.server

deviex: ## Open an IEx session on the running local app. If entr is installed, also watch files and auto-recompile on changes.
	iex --name console-$$(openssl rand -hex 4)@127.0.0.1 --remsh sequin-stream-dev@127.0.0.1 --cookie sequin-stream-dev

watch: ## Watch files and auto-recompile on changes using entr
	git ls-files | entr mix compile

deviex2: ## Open a second IEx session on the running local app
	iex --name console-$$(openssl rand -hex 4)@127.0.0.1 --remsh sequin-stream-dev2@127.0.0.1 --cookie sequin-stream-dev

signoff: ## Run the signoff script [exclude=<tag>]
	@if [ -n "$(exclude)" ]; then \
		./scripts/signoff.sh --exclude $(exclude); \
	else \
		./scripts/signoff.sh; \
	fi

signoff-dirty: ## Run the signoff script with --dirty flag [exclude=<tag>]
	@if [ -n "$(exclude)" ]; then \
		./scripts/signoff.sh --dirty --exclude $(exclude); \
	else \
		./scripts/signoff.sh --dirty; \
	fi

signoff-pr: ## Run signoff on a PR by providing PR number: make signoff-pr pr=1234
	@if [ -z "$(pr)" ]; then \
		echo "Usage: make signoff-pr pr=<pr-number>"; \
	else \
		./scripts/signoff_pr.sh $(pr); \
	fi

signoff-stack: ## Run the signoff-stack script
	@./scripts/signoff_stack.sh

merge: ## Run the merge script
	@./scripts/merge.sh

merge-force: ## Run the merge script, bypassing signoff check
	@./scripts/merge.sh --force

init: ## Create .settings.json from .settings.json.example
	@if [ ! -f .settings.json ]; then \
		cp .settings.example.json .settings.json; \
		echo ".settings.json created. Please edit it to configure your settings."; \
	else \
		echo ".settings.json already exists. No changes made."; \
	fi

release: ## Run the release script
	@./scripts/release.sh

release-dirty: ## Run the release script with --dirty flag
	@./scripts/release.sh --dirty

release-gh: ## Run the release script using GitHub Actions for Docker builds
	@./scripts/release.sh --github-actions

release-dirty-gh: ## Run the release script with --dirty flag using GitHub Actions
	@./scripts/release.sh --dirty --github-actions

checkout-pr: ## Check out a PR locally: make checkout-pr pr=<pr-number>
	@if [ -z "$(pr)" ]; then \
		echo "Usage: make checkout-pr pr=<pr-number>"; \
	else \
		./scripts/checkout_pr.sh $(pr); \
	fi

delete-branch: ## Delete the current branch and its non‑origin remote
	@./scripts/delete_branch.sh

spellcheck: ## Run cspell to check spelling in .md and .mdx files
	@npx -y cspell "**/*.{md,mdx}" --config spellcheck/.cspell.json --exclude "**/node_modules/**"

addword: ## Add a word to project-words.txt
	@if [ -z "$(word)" ]; then \
		echo "Usage: make addword word=<word>"; \
	else \
		echo "$(word)" >> spellcheck/project-words.txt; \
		sort -u spellcheck/project-words.txt -o spellcheck/project-words.txt; \
		echo "Added '$(word)' to project-words.txt"; \
	fi

check-links: ## Run mintlify broken-links in the docs directory
	@cd docs && npx mintlify broken-links

buildpush: ## Run mix buildpush (build and push docker image)
	mix buildpush

buildpush-gh: ## Run GitHub workflow in watch mode
	gh workflow run docker-cloud-build.yml --ref main
	sleep 3
	gh run watch $(gh run list --workflow=docker-cloud-build.yml --limit 1 --json databaseId --jq '.[0].databaseId')

buildpush-dirty: ## Run mix buildpush with --dirty flag
	mix buildpush --dirty

impersonate: ## Generate impersonate link for impersonated user from .settings.json
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_rpc.sh "Sequin.Accounts.Impersonate.generate_link\(~s{$(impersonating_user_id)}\,~s{$(impersonated_user_id)}\)"

deploy: ## Deploy the specified [sha=<commit-sha>] or latest commit
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/deploy.sh $(sha)

remiex: ## Drop into remote iex session
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_sequin.sh remote

connectdb: ## Connect to the production database [id=<id>] [open=<open>]
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_db.sh $(id) $(open)

connect: ## Connect to the production database
	@INFRA_DIR=$$(jq -r '.infraDir // "../infra"' .settings.json); \
	cd "$$INFRA_DIR" && ./scripts/prod_ssh.sh

docs: ## Run mintlify dev server for documentation
	@cd docs && mintlify dev

redis-console-consumer: ## Read from redis stream <stream-key> [from-beginning]
	@./scripts/redis-console-consumer.sh $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

