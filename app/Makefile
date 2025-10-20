.NO_PRINT_DIRECTORY:

# Nombre de la imagen y el contenedor
IMAGE_NAME = image-script-app
CONTAINER_NAME = container-script-app

# Puerto que expone
PORT = 5151

# Default target
.DEFAULT_GOAL := help

## Build: Construir la imagen
build:
	docker build -t $(IMAGE_NAME) .

## Run-dev: Correr el contenedor mapeando el código para desarrollo (hot reload)
run:
	docker run -d --name $(CONTAINER_NAME) -p $(PORT):$(PORT) -v $(shell pwd):/app $(IMAGE_NAME)

## Listar comandos disponibles
commands:
	docker run --rm $(IMAGE_NAME)

## Benchmark: Ejecutar comandos de benchmark pasados como argumento
bench:
	docker run --network mynet --rm $(IMAGE_NAME) python main.py $(ARGS)

## Exec: Entrar al contenedor con bash
exec:
	docker exec -it $(CONTAINER_NAME) bash

## Start: Iniciar el contenedor detenido
start:
	docker start $(CONTAINER_NAME)

## Stop: Detener el contenedor en ejecución
stop:
	docker stop $(CONTAINER_NAME)

## Remove: Eliminar el contenedor
rm:
	docker rm -f $(CONTAINER_NAME)

## Remove image: Eliminar la imagen creada
rmi:
	docker rmi -f $(IMAGE_NAME)

## PS: Listar contenedores en ejecución
ps:
	docker ps

## Logs: Ver logs del contenedor
logs:
	docker logs -f $(CONTAINER_NAME)

## Help: Mostrar ayuda
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //'
