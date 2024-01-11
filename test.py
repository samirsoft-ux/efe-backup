import psycopg2
import ibm_boto3
from ibm_botocore.client import Config
from datetime import datetime
import os
import subprocess

# Conectar a PostgreSQL

PG_HOST = os.environ.get("PG_HOST")
PG_PORT = int(os.environ.get("PG_PORT", 31174))
PG_DATABASE = os.environ.get("PG_DATABASE")
PG_USER = os.environ.get("PG_USER")
PGPASSWORD = os.environ.get("PGPASSWORD")
FECHAYHORA = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
PG_BACKUP_FILENAME = f"./fullbackup_{PG_DATABASE}_{FECHAYHORA}.backup"

try:
    # Realizar Full Backup

    command = [
        "pg_dump",
        "-h", PG_HOST,
        "-p", str(PG_PORT),
        "-U", PG_USER,
        "-F", "c",  
        "-f", PG_BACKUP_FILENAME,  
        "-d", PG_DATABASE 
    ]
    # Establecer la variable de entorno PGPASSWORD
    os.environ["PGPASSWORD"] = PGPASSWORD

    subprocess.run(command, check=True)
    print("Backup completo realizado con éxito")

except Exception as e:
    print("Error durante el backup de PostgreSQL:", e)

# Enviar full backup al COS

APIKEY = os.environ.get("APIKEY")
ENDPOINT = os.environ.get("ENDPOINT")
SERVICE_INSTANCE_ID = os.environ.get("SERVICE_INSTANCE_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
BACKUP_OBJECT_NAME = f"fullbackup_{PG_DATABASE}_{FECHAYHORA}.backup"

# Configuración del cliente para IBM COS

try:

    cos = ibm_boto3.resource("s3",
        ibm_api_key_id=APIKEY,
        ibm_service_instance_id=SERVICE_INSTANCE_ID,
        config=Config(signature_version="oauth"),
        endpoint_url=ENDPOINT
    )

    with open(PG_BACKUP_FILENAME, "rb") as file_data:
        cos.Object(BUCKET_NAME, BACKUP_OBJECT_NAME).upload_fileobj(file_data)
    print("Backup subido con éxito a IBM COS")

except Exception as e:
    print("Un error ocurrió durante la subida a IBM COS:", e)

del os.environ["PGPASSWORD"]