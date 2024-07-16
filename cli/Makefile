.PHONY: build release

build:
	@echo "Building and installing sequin..."
	@bash build_and_install.sh

release:
	@echo "Running release process..."
	@bash release.sh

help:
	@echo "Available commands:"
	@echo "  make build   - Build and install the sequin binary"
	@echo "  make release - Run the release process"
	@echo "  make help    - Show this help message"

.DEFAULT_GOAL := help