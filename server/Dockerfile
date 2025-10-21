FROM mariadb:10.5

COPY my.cnf /etc/mysql/conf.d/general-log.cnf

COPY init/enable_performance_schema.sql /docker-entrypoint-initdb.d/


RUN ls