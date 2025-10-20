FROM percona/percona-server:8.0

USER root

# Actualizar e instalar herramientas necesarias para agregar repositorios
RUN apt-get update && apt-get install -y wget gnupg lsb-release

# Agregar repositorio oficial de Percona para toolkit
RUN wget https://repo.percona.com/apt/percona-release_latest.$(lsb_release -sc)_all.deb && \
    dpkg -i percona-release_latest.$(lsb_release -sc)_all.deb && \
    percona-release enable tools release && \
    apt-get update

# Instalar percona-toolkit
RUN apt-get install -y percona-toolkit && apt-get clean && rm -rf /var/lib/apt/lists/*

CMD ["mysqld"]
