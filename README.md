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

# Consultar eventos de consultas lentas:
SELECT 
    event_id,
    event_name,
    sql_text,
    ROUND((timer_end - timer_start) / 1000000000, 3) AS duration_ms
FROM performance_schema.events_statements_history_long
WHERE sql_text IS NOT NULL
ORDER BY event_id DESC
LIMIT 10;