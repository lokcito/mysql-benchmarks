import click
import random
import time
from faker import Faker
from sqlalchemy import create_engine, text

DB_NAME = "midb"
DB_USER = "root"
DB_PASSWORD = "secret123"
DB_HOST = "container-mysql-server"
DB_PORT = 3306

fake = Faker()

# DDL de tablas
TABLES = {
    "Usuarios": """
        CREATE TABLE IF NOT EXISTS Usuarios (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(100),
            email VARCHAR(100) UNIQUE,
            telefono VARCHAR(20),
            direccion VARCHAR(200),
            fecha_registro DATETIME
        )
    """,
    "Productos": """
        CREATE TABLE IF NOT EXISTS Productos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(100),
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
            metodo_pago VARCHAR(50),
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
def reset():
    """Elimina la DB y la recrea"""
    with base_engine().connect() as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {DB_NAME}"))
        conn.commit()
    click.echo("🗑️ Base de datos eliminada ✅")

if __name__ == "__main__":
    cli()