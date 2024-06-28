.PHONY: build

build:
	@echo "Building and installing sequin..."
	@bash build_and_install.sh

help:
	@echo "Available commands:"
	@echo "  make build  - Build and install the sequin binary"
	@echo "  make help   - Show this help message"

.DEFAULT_GOAL := help