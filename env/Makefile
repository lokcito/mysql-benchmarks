# Default target
.DEFAULT_GOAL := help

## Network: Crear una red
network:
	docker network create mynet

## Tmux: Ejecutar start.sh
screens:
	apk add tmux && chmod +x ./screens.sh && ./screens.sh

## Help: Mostrar ayuda
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //'