import click
import random
import time
from faker import Faker
from sqlalchemy import create_engine, text

DB_NAME = "mi_gran_db"
DB_USER = "root"
DB_PASSWORD = "secret123"
DB_HOST = "container-percona-server"
DB_PORT = 3307

fake = Faker()

# DDL de tablas
TABLES = {
    "Usuarios": """
        CREATE TABLE IF NOT EXISTS Usuarios (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(250),
            email VARCHAR(250) UNIQUE,
            telefono VARCHAR(250),
            direccion VARCHAR(250),
            fecha_registro DATETIME
        )
    """,
    "Productos": """
        CREATE TABLE IF NOT EXISTS Productos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(250),
            descripcion TEXT,
            precio DECIMAL(10,2),
            stock INT,
            fecha_creacion DATETIME
        )
    """,
    "UsuarioProductos": """
        CREATE TABLE IF NOT EXISTS UsuarioProductos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            usuario_id INT,
            producto_id INT,
            cantidad INT,
            fecha_compra DATETIME,
            metodo_pago VARCHAR(250),
            FOREIGN KEY (usuario_id) REFERENCES Usuarios(id),
            FOREIGN KEY (producto_id) REFERENCES Productos(id)
        )
    """,
}

# Conexión base (sin DB) y con DB
def base_engine():
    return create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/")

def db_engine():
    return create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def database_exists():
    with base_engine().connect() as conn:
        result = conn.execute(text("SHOW DATABASES LIKE :db"), {"db": DB_NAME}).fetchone()
        return result is not None

def create_database():
    with base_engine().connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        conn.commit()

def create_tables():
    with db_engine().connect() as conn:
        for ddl in TABLES.values():
            conn.execute(text(ddl))
        conn.commit()

def count_records():
    counts = {}
    with db_engine().connect() as conn:
        for table in TABLES.keys():
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            counts[table] = result[0]
    return counts

def populate_data(sleep_time=0, target_size_mb=20):
    target_bytes = target_size_mb * 1024 * 1024
    approx_size_per_row = 500
    num_rows = target_bytes // approx_size_per_row

    num_usuarios = num_rows // 10
    num_productos = num_rows // 20
    num_relaciones = num_rows

    click.echo(f"Poblando {num_usuarios} usuarios, {num_productos} productos, {num_relaciones} relaciones...")

    with db_engine().begin() as conn:  # begin = auto commit/rollback
        # Usuarios
        for _ in range(num_usuarios):
            conn.execute(
                text("INSERT INTO Usuarios (nombre, email, telefono, direccion, fecha_registro) VALUES (:n,:e,:t,:d,NOW())"),
                {"n": fake.name(), "e": fake.unique.email(), "t": fake.phone_number(), "d": fake.address()},
            )
            if sleep_time > 0:
                time.sleep(sleep_time)

        # Productos
        for _ in range(num_productos):
            conn.execute(
                text("INSERT INTO Productos (nombre, descripcion, precio, stock, fecha_creacion) VALUES (:n,:d,:p,:s,NOW())"),
                {
                    "n": fake.word(),
                    "d": fake.text(max_nb_chars=200),
                    "p": round(random.uniform(10, 500), 2),
                    "s": random.randint(1, 1000),
                },
            )
            if sleep_time > 0:
                time.sleep(sleep_time)

        # UsuarioProductos
        usuarios_ids = [row[0] for row in conn.execute(text("SELECT id FROM Usuarios")).fetchall()]
        productos_ids = [row[0] for row in conn.execute(text("SELECT id FROM Productos")).fetchall()]

        for _ in range(num_relaciones):
            conn.execute(
                text("INSERT INTO UsuarioProductos (usuario_id, producto_id, cantidad, fecha_compra, metodo_pago) VALUES (:u,:p,:c,NOW(),:m)"),
                {
                    "u": random.choice(usuarios_ids),
                    "p": random.choice(productos_ids),
                    "c": random.randint(1, 5),
                    "m": random.choice(["efectivo", "tarjeta", "yape", "plin"]),
                },
            )
            if sleep_time > 0:
                time.sleep(sleep_time)


@click.group()
def cli():
    pass

@cli.command()
def init():
    """Crea la DB y tablas si no existen"""
    if database_exists():
        click.echo("⚠️  La base de datos ya existe. No se creó nada.")
    else:
        create_database()
        create_tables()
        click.echo("✅ Base de datos y tablas creadas.")

@cli.command()
@click.option("--sleep", default=0, help="Tiempo en segundos para pausar entre inserts.")
def populate(sleep):
    """Pobla la DB hasta ~20MB o muestra registros si ya hay"""
    counts = count_records()
    if all(v > 0 for v in counts.values()):
        click.echo(f"⚠️  La DB ya tiene datos: {counts}")
    else:
        populate_data(sleep_time=sleep)
        counts = count_records()
        click.echo(f"✅ Poblado terminado: {counts}")

@cli.command()
def slow_select():
    """Consulta SELECT continua cada 5 segundos para simular carga de lectura"""
    click.echo("Iniciando consultas SELECT continuas cada 5 segundos...")
    try:
        with db_engine().connect() as conn:
            while True:
                result = conn.execute(text("SELECT COUNT(*) FROM Usuarios")).fetchone()
                click.echo(f"Usuarios count: {result[0]}")
                time.sleep(5)
    except KeyboardInterrupt:
        click.echo("Interrumpido por usuario.")

@cli.command()
def slow_update():
    """Updates pausados cada 5 segundos en filas aleatorias"""
    click.echo("Iniciando updates pausados cada 5 segundos en Usuarios...")
    try:
        with db_engine().begin() as conn:
            usuario_ids = [row[0] for row in conn.execute(text("SELECT id FROM Usuarios")).fetchall()]
        while True:
            with db_engine().begin() as conn:
                uid = random.choice(usuario_ids)
                new_phone = fake.phone_number()
                conn.execute(
                    text("UPDATE Usuarios SET telefono = :phone WHERE id = :id"),
                    {"phone": new_phone, "id": uid}
                )
                click.echo(f"Actualizado telefono para usuario id {uid} a {new_phone}")
            time.sleep(5)
    except KeyboardInterrupt:
        click.echo("Interrumpido por usuario.")

@cli.command()
def slow_insert():
    """Inserts pausados cada 5 segundos en UsuarioProductos"""
    click.echo("Iniciando inserts pausados cada 5 segundos en UsuarioProductos...")
    try:
        with db_engine().begin() as conn:
            usuario_ids = [row[0] for row in conn.execute(text("SELECT id FROM Usuarios")).fetchall()]
            productos_ids = [row[0] for row in conn.execute(text("SELECT id FROM Productos")).fetchall()]
        while True:
            with db_engine().begin() as conn:
                conn.execute(
                    text("INSERT INTO UsuarioProductos (usuario_id, producto_id, cantidad, fecha_compra, metodo_pago) VALUES (:u,:p,:c,NOW(),:m)"),
                    {
                        "u": random.choice(usuario_ids),
                        "p": random.choice(productos_ids),
                        "c": random.randint(1, 5),
                        "m": random.choice(["efectivo", "tarjeta", "yape", "plin"]),
                    }
                )
                click.echo("Insert en UsuarioProductos realizado.")
            time.sleep(5)
    except KeyboardInterrupt:
        click.echo("Interrumpido por usuario.")

@cli.command()
def mixed_load():
    """Ejecuta en paralelo updates, selects simples y selects con joins cada 5 segundos"""
    click.echo("Iniciando carga mixta (update, select simple, select con join) cada 5 segundos...")
    try:
        with db_engine().begin() as conn:
            usuario_ids = [row[0] for row in conn.execute(text("SELECT id FROM Usuarios")).fetchall()]
            producto_ids = [row[0] for row in conn.execute(text("SELECT id FROM Productos")).fetchall()]
        while True:
            with db_engine().begin() as conn:
                # UPDATE en Usuarios - cambio telefono
                uid = random.choice(usuario_ids)
                new_phone = fake.phone_number()
                conn.execute(
                    text("UPDATE Usuarios SET telefono = :phone WHERE id = :id"),
                    {"phone": new_phone, "id": uid}
                )
                click.echo(f"Update: Teléfono usuario_id={uid} a {new_phone}")

                # SELECT simple - contar productos
                prod_count = conn.execute(text("SELECT COUNT(*) FROM Productos")).fetchone()[0]
                click.echo(f"Select simple: Total productos = {prod_count}")

                # SELECT con JOIN - productos comprados por un usuario aleatorio
                uid_join = random.choice(usuario_ids)
                join_result = conn.execute(
                    text("""
                        SELECT p.nombre, up.cantidad, up.metodo_pago
                        FROM UsuarioProductos up
                        JOIN Productos p ON up.producto_id = p.id
                        WHERE up.usuario_id = :uid
                        LIMIT 3
                    """),
                    {"uid": uid_join}
                ).fetchall()
                click.echo(f"Select join: Productos del usuario_id={uid_join}: {join_result}")
            time.sleep(5)
    except KeyboardInterrupt:
        click.echo("Interrumpido por usuario.")


@cli.command()
def reset():
    """Elimina la DB y la recrea"""
    with base_engine().connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.commit()
    click.echo("🗑️ Base de datos eliminada ✅")

if __name__ == "__main__":
    cli()