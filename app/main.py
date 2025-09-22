import click
import random
import time
import mysql.connector
from faker import Faker

DB_NAME = "miappdb"

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

fake = Faker()


def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",  # 👈 ajusta tu password
    )


def database_exists(cursor):
    cursor.execute("SHOW DATABASES LIKE %s", (DB_NAME,))
    return cursor.fetchone() is not None


def create_database(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cursor.execute(f"USE {DB_NAME}")


def create_tables(cursor):
    for name, ddl in TABLES.items():
        cursor.execute(ddl)


def count_records(cursor):
    counts = {}
    for table in TABLES.keys():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        counts[table] = cursor.fetchone()[0]
    return counts


def populate_data(cursor, sleep_time=0, target_size_mb=20):
    target_bytes = target_size_mb * 1024 * 1024
    approx_size_per_row = 500
    num_rows = target_bytes // approx_size_per_row

    num_usuarios = num_rows // 10
    num_productos = num_rows // 20
    num_relaciones = num_rows

    click.echo(f"Poblando {num_usuarios} usuarios, {num_productos} productos, {num_relaciones} relaciones...")

    # Usuarios
    for _ in range(num_usuarios):
        cursor.execute(
            "INSERT INTO Usuarios (nombre, email, telefono, direccion, fecha_registro) VALUES (%s,%s,%s,%s,NOW())",
            (fake.name(), fake.unique.email(), fake.phone_number(), fake.address())
        )
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Productos
    for _ in range(num_productos):
        cursor.execute(
            "INSERT INTO Productos (nombre, descripcion, precio, stock, fecha_creacion) VALUES (%s,%s,%s,%s,NOW())",
            (fake.word(), fake.text(max_nb_chars=200), round(random.uniform(10, 500), 2), random.randint(1, 1000))
        )
        if sleep_time > 0:
            time.sleep(sleep_time)

    # UsuarioProductos
    cursor.execute("SELECT id FROM Usuarios")
    usuarios_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT id FROM Productos")
    productos_ids = [row[0] for row in cursor.fetchall()]

    for _ in range(num_relaciones):
        cursor.execute(
            "INSERT INTO UsuarioProductos (usuario_id, producto_id, cantidad, fecha_compra, metodo_pago) VALUES (%s,%s,%s,NOW(),%s)",
            (
                random.choice(usuarios_ids),
                random.choice(productos_ids),
                random.randint(1, 5),
                random.choice(["efectivo", "tarjeta", "yape", "plin"]),
            )
        )
        if sleep_time > 0:
            time.sleep(sleep_time)


@click.group()
def cli():
    pass


@cli.command()
def init():
    """Crea la DB y tablas si no existen"""
    conn = connect_db()
    cur = conn.cursor()

    if database_exists(cur):
        click.echo("⚠️  La base de datos ya existe. No se creó nada.")
    else:
        create_database(cur)
        create_tables(cur)
        conn.commit()
        click.echo("✅ Base de datos y tablas creadas.")

    conn.close()


@cli.command()
@click.option("--sleep", default=0, help="Tiempo en segundos para pausar entre inserts.")
def populate(sleep):
    """Pobla la DB hasta ~20MB o muestra registros si ya hay"""
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(f"USE {DB_NAME}")

    counts = count_records(cur)
    if all(v > 0 for v in counts.values()):
        click.echo(f"⚠️  La DB ya tiene datos: {counts}")
    else:
        populate_data(cur, sleep_time=sleep)
        conn.commit()
        counts = count_records(cur)
        click.echo(f"✅ Poblado terminado: {counts}")

    conn.close()


@cli.command()
def reset():
    """Elimina la DB y la recrea"""
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
    conn.commit()
    conn.close()
    click.echo("🗑️ Base de datos eliminada ✅")


if __name__ == "__main__":
    cli()