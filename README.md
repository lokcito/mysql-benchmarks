# Ingresar a MariaDB:
```
mariadb -h container-mysql-server -P 3306 -u root -psecret123
```

# Consultar procesos MySQL:
```
show processlist;
```

# Consultar variables de rendimiento:
```
SHOW STATUS LIKE 'Threads%';
SHOW GLOBAL STATUS LIKE 'Questions';
```

# Activar log de consultas lentas:
```
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 1;  -- en segundos
SHOW VARIABLES LIKE 'slow_query%';
```