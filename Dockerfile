# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.9
FROM python:${PYTHON_VERSION}-slim as base

# Evita que Python escriba archivos pyc
ENV PYTHONDONTWRITEBYTECODE=1

# Evita que Python almacene en buffer stdout y stderr
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Crear un usuario no privilegiado para ejecutar la aplicación
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/app" \
    --uid "${UID}" \
    appuser

RUN chown appuser:appuser /app

# Instalar cliente de PostgreSQL y actualizar pip
RUN apt-get update && apt-get install -y postgresql-client && rm -rf /var/lib/apt/lists/* && \
    pip install --upgrade pip

# Instalar dependencias de Python directamente con pip, incluyendo el SDK de IBM Secrets Manager
RUN pip install --no-cache-dir psycopg2-binary ibm-cos-sdk "ibm-secrets-manager-sdk"

# Cambiar al usuario no privilegiado para ejecutar la aplicación
USER appuser

# Copiar solo el archivo test.py en el contenedor
COPY test.py .

# Exponer el puerto que utiliza la aplicación, si es necesario
EXPOSE 5000

# Ejecutar la aplicación
CMD ["python", "test.py"]
