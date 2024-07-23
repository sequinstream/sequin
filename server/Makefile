.PHONY: help

APP_NAME ?= sequin
APP_VSN ?= `grep 'version:' mix.exs | cut -d '"' -f2`
BUILD ?= `git rev-parse --short HEAD`

buildx: ## Build the Docker image for prod
	docker buildx build --load --platform=linux/arm64/v8 \
		-t $(APP_NAME):$(APP_VSN)-$(BUILD) \
		-t $(APP_NAME):latest .

build: ## Build the Docker image
	docker build \
		-t $(APP_NAME):$(APP_VSN)-$(BUILD) \
		-t $(APP_NAME):latest .

dev: ## Run the app locally
	elixir --sname sequin-stream-dev --cookie sequin-stream-dev -S mix phx.server

deviex: ## Open an IEx session on the running local app
	iex --sname console-$$(openssl rand -hex 4) --remsh sequin-stream-dev --cookie sequin-stream-dev

signoff:
	mix signoff