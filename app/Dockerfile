# Usa una imagen base ligera de Python
FROM python:3.11-slim

# Evitar buffering en logs
ENV PYTHONUNBUFFERED=1

# Crear directorio de la app
WORKDIR /app

# Instalar dependencias del sistema (ej: para psycopg2 o mysqlclient si usas MySQL)
RUN apt-get update && apt-get install -y \
    default-mysql-client \
    gcc \
    libmariadb-dev \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la app
COPY . .

# Comando por defecto (para ver ayuda del CLI)
CMD ["python", "main.py", "--help"]