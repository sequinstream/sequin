.PHONY: build build-dev build-prod release

build: build-dev

build-dev:
	@echo "Building and installing sequin (development version)..."
	@bash build_and_install.sh

build-prod:
	@echo "Building and installing sequin (production version)..."
	@bash build_and_install.sh prod

release:
	@echo "Running release process..."
	@bash release.sh

release-dirty:
	@echo "Running release process..."
	@bash release.sh --dirty

help:
	@echo "Available commands:"
	@echo "  make build      - Build and install the sequin binary (development version)"
	@echo "  make build-dev  - Same as 'make build'"
	@echo "  make build-prod - Build and install the sequin binary (production version)"
	@echo "  make release    - Run the release process"
	@echo "  make help       - Show this help message"

.DEFAULT_GOAL := help