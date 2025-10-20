FROM percona:10.5

# Exponer puerto diferente, por ejemplo el 3307 en lugar del 3306
EXPOSE 3307

# Instalar Percona Toolkit
RUN apt-get update && \
    apt-get install -y percona-toolkit && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

CMD ["mysqld", "--port=3307"]
