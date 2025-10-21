import click
import random
import time
from faker import Faker
from sqlalchemy import create_engine, text
import string

DB_NAME = "escuela_db"
DB_USER = "root"
DB_PASSWORD = "secret123"
DB_HOST = "container-mysql-server"
DB_PORT = 3306

fake = Faker()

# DDL de tablas
TABLES = {
    "carrera": """
        CREATE TABLE carrera (
            id_carrera INT PRIMARY KEY AUTO_INCREMENT,
            nombre VARCHAR(100)
        )
    """,
    "estudiante": """
        CREATE TABLE estudiante (
            id_estudiante INT PRIMARY KEY AUTO_INCREMENT,
            nombre VARCHAR(100),
            id_carrera INT,
            FOREIGN KEY (id_carrera) REFERENCES carrera(id_carrera)
        )
    """,
    "curso": """
        CREATE TABLE curso (
            id_curso INT PRIMARY KEY AUTO_INCREMENT,
            nombre VARCHAR(100),
            creditos INT
        )    
    """,
    "matricula": """
        CREATE TABLE matricula (
            id_matricula INT PRIMARY KEY AUTO_INCREMENT,
            id_estudiante INT,
            id_curso INT,
            fecha DATE,
            FOREIGN KEY (id_estudiante) REFERENCES estudiante(id_estudiante),
            FOREIGN KEY (id_curso) REFERENCES curso(id_curso)
        )
    """,
    "nota": """
        CREATE TABLE nota (
            id_nota INT PRIMARY KEY AUTO_INCREMENT,
            id_matricula INT,
            nota_final DECIMAL(4,2),
            FOREIGN KEY (id_matricula) REFERENCES matricula(id_matricula)
        )    
    """
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
    num_rows = int(target_bytes // approx_size_per_row)

    num_carreras = 50
    num_cursos = 500
    num_estudiantes = num_rows
    num_matriculas = num_estudiantes * 3  # promedio 3 por estudiante

    click.echo(f"Poblando {num_carreras} carreras, {num_cursos} cursos, {num_estudiantes} estudiantes, {num_matriculas} matrículas...")

    with db_engine().begin() as conn:
        # -------------------
        # 1️⃣ Carreras
        # -------------------
        for i in range(1, num_carreras + 1):
            conn.execute(
                text("INSERT INTO carrera (nombre) VALUES (:n)"),
                {"n": f"Carrera {i}"}
            )
        click.echo("✅ Carreras insertadas")

        # -------------------
        # 2️⃣ Cursos
        # -------------------
        for i in range(1, num_cursos + 1):
            conn.execute(
                text("INSERT INTO curso (nombre, creditos) VALUES (:n, :c)"),
                {
                    "n": f"Curso_{i}_{random.choice(string.ascii_uppercase)}",
                    "c": random.randint(1, 5)
                }
            )
        click.echo("✅ Cursos insertados")

        # -------------------
        # 3️⃣ Estudiantes
        # -------------------
        for i in range(num_estudiantes):
            conn.execute(
                text("INSERT INTO estudiante (nombre, id_carrera) VALUES (:n, :c)"),
                {
                    "n": f"Estudiante_{i}_{random.choice(string.ascii_lowercase)}",
                    "c": random.randint(1, num_carreras)
                }
            )
            if sleep_time > 0:
                time.sleep(sleep_time)
        click.echo("✅ Estudiantes insertados")

        # -------------------
        # 4️⃣ Matrículas
        # -------------------
        carreras_ids = [row[0] for row in conn.execute(text("SELECT id FROM carrera")).fetchall()]
        cursos_ids = [row[0] for row in conn.execute(text("SELECT id FROM curso")).fetchall()]
        estudiantes_ids = [row[0] for row in conn.execute(text("SELECT id FROM estudiante")).fetchall()]

        for est_id in estudiantes_ids:
            for _ in range(random.randint(1, 5)):
                conn.execute(
                    text("INSERT INTO matricula (id_estudiante, id_curso, fecha) VALUES (:e,:c,:f)"),
                    {
                        "e": est_id,
                        "c": random.choice(cursos_ids),
                        "f": f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
                    }
                )
                if sleep_time > 0:
                    time.sleep(sleep_time)
        click.echo("✅ Matrículas insertadas")

        # -------------------
        # 5️⃣ Notas
        # -------------------
        matriculas_ids = [row[0] for row in conn.execute(text("SELECT id_matricula FROM matricula")).fetchall()]
        for mid in matriculas_ids:
            conn.execute(
                text("INSERT INTO nota (id_matricula, nota_final) VALUES (:m, :n)"),
                {"m": mid, "n": round(random.uniform(0, 20), 2)}
            )
            if sleep_time > 0:
                time.sleep(sleep_time)
        click.echo("✅ Notas insertadas")

    click.echo("🎉 Poblamiento completado exitosamente.")

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
def query_1():
    """
    Obtiene a los estudiantes con nota final entre 10 y 15,
    """
    click.echo("Iniciando consulta...")
    try:
        with db_engine().connect() as conn:
            while True:
                # Obtener todos los id de matrícula con notas entre 10 y 15
                matriculas = conn.execute(text("""
                    SELECT id_matricula 
                    FROM nota 
                    WHERE nota_final BETWEEN 10 AND 15;
                """)).fetchall()

                total_estudiantes = set()  # para evitar duplicados

                # Por cada matrícula, obtener el estudiante con subconsultas
                for (id_matricula,) in matriculas:
                    # ❌ Subconsulta para obtener id_estudiante
                    estudiante_row = conn.execute(text(f"""
                        SELECT id_estudiante 
                        FROM matricula 
                        WHERE id_matricula = {id_matricula};
                    """)).fetchone()

                    if estudiante_row:
                        id_estudiante = estudiante_row[0]

                        # ❌ Subconsulta adicional para obtener nombre del estudiante
                        nombre_row = conn.execute(text(f"""
                            SELECT nombre 
                            FROM estudiante 
                            WHERE id_estudiante = {id_estudiante};
                        """)).fetchone()

                        if nombre_row:
                            total_estudiantes.add(nombre_row[0])

                click.echo(f"🎓 Estudiantes con nota entre 10 y 15: {len(total_estudiantes)} encontrados")
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