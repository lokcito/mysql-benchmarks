# Default target
.DEFAULT_GOAL := help

## Network: Crear una red
network:
	docker network create mynet

## Help: Mostrar ayuda
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //'