# Nombre de la imagen y el contenedor
IMAGE_NAME = image-mysql-server
CONTAINER_NAME = container-mysql-server

# Nuevas variables para phpMyAdmin
PMA_CONTAINER_NAME=mi-phpmyadmin
PMA_PORT=8080

# Puerto que expone
PORT = 3306

# Default target
.DEFAULT_GOAL := help

## Build: Construir la imagen
build:
	docker build -t $(IMAGE_NAME) .

## Run: Correr el contenedor en segundo plano
run-db:
	docker run --network mynet -d --name $(CONTAINER_NAME) -e MARIADB_ROOT_PASSWORD=secret123 \
  -e MARIADB_DATABASE=midb \
  -e MARIADB_USER=usuario \
  -e MARIADB_PASSWORD=clave123 \
  -p $(PORT):$(PORT) $(IMAGE_NAME)

# Nuevo comando para phpMyAdmin
run-pma:
	docker run --network mynet -d --name $(PMA_CONTAINER_NAME) \
		-e PMA_HOST=$(CONTAINER_NAME) \
		-e PMA_PORT=3306 \
		-p $(PMA_PORT):80 \
		phpmyadmin/phpmyadmin:latest

## Exec: Entrar al contenedor con bash
exec:
	docker exec -it $(CONTAINER_NAME) bash

## Start: Iniciar el contenedor detenido
start:
	docker start $(CONTAINER_NAME)

## Stop: Detener el contenedor en ejecución
stop:
	docker stop $(CONTAINER_NAME) $(PMA_CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) $(PMA_CONTAINER_NAME) || true

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

## Run all: Correr el contenedor en segundo plano
run-all: run-db
	sleep 5
	$(MAKE) run-pma

## Help: Mostrar ayuda
help:
	@grep -E '^##' $(MAKEFILE_LIST) | sed -e 's/## //'