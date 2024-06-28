.PHONY: help

APP_NAME ?= sequin
APP_VSN ?= `grep 'version:' mix.exs | cut -d '"' -f2`
BUILD ?= `git rev-parse --short HEAD`

help:
	@echo "$(APP_NAME):$(APP_VSN)-$(BUILD)"
	@perl -nle'print $& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

buildx: ## Build the Docker image for prod
	docker buildx build --load --platform=linux/arm64/v8 \
		-t $(APP_NAME):$(APP_VSN)-$(BUILD) \
		-t $(APP_NAME):latest .

build: ## Build the Docker image
	docker build \
		-t $(APP_NAME):$(APP_VSN)-$(BUILD) \
		-t $(APP_NAME):latest .

run: ## Run the app in Docker
	docker run --env-file config/docker.env \
	--expose 4000 -p 4000:4000 \
	--expose 5432 -p 5432:5432 \
	--rm -it $(APP_NAME):latest

dev: ## Run the app locally
	elixir --sname sequin-stream-dev --cookie sequin-stream-dev -S mix phx.server

deviex: ## Open an IEx session on the running local app
	iex --sname console-$$(openssl rand -hex 4) --remsh sequin-stream-dev --cookie sequin-stream-dev

connect:
	scripts/prod_ssh.sh

remiex:
	scripts/prod_ix.sh remote