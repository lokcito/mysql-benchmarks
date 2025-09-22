# Nombre de la imagen y el contenedor
IMAGE_NAME = image-mysql-server
CONTAINER_NAME = container-mysql-server

# Puerto que expone
PORT = 3306

# Default target
.DEFAULT_GOAL := help

## Build: Construir la imagen
build:
	docker build -t $(IMAGE_NAME) .

## Run: Correr el contenedor en segundo plano
run:
	docker run -d --name $(CONTAINER_NAME) -p $(PORT):$(PORT) $(IMAGE_NAME)

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