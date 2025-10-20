# Usamos una imagen base de Debian slim
FROM debian:bullseye-slim

# Actualizamos los paquetes y luego instalamos los paquetes necesarios
RUN apt-get update && apt-get install -y \
    mariadb-client \
    percona-toolkit \
    bash \
    curl \
    && apt-get clean

# Establecemos el directorio de trabajo en /workspace
WORKDIR /workspace

# Definimos el comando por defecto para iniciar un shell interactivo
CMD [ "bash" ]