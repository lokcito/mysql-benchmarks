.NO_PRINT_DIRECTORY:

# Nombre de la imagen y el contenedor
IMAGE_NAME = image-percona-server
CONTAINER_NAME = container-percona-server

# Puerto que expone y usa internamente (puedes cambiarlo para evitar conflictos)
PORT = 3307

# Default target
.DEFAULT_GOAL := help

## Build: Construir la imagen
build:
	docker build -t $(IMAGE_NAME) .

## Run: Correr el contenedor en segundo plano con red personalizada y puerto mapeado
run:
	docker run --network mynet -d --name $(CONTAINER_NAME) -e MYSQL_ROOT_PASSWORD=secret123 \
  -e MYSQL_DATABASE=midb \
  -e MYSQL_USER=usuario \
  -e MYSQL_PASSWORD=clave123 \
  -p $(PORT):$(PORT) $(IMAGE_NAME) mysqld --port=$(PORT)

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
