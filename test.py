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
PG_FILENAME = f"./backup_logs_{FECHAYHORA}.csv"
PG_BACKUP_FILENAME = f"./fullbackup_{PG_DATABASE}_{FECHAYHORA}.backup"

try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PGPASSWORD
    )

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
    
    # Crear CSV basado en query

    cur = conn.cursor()

    query_export = """
    COPY (
        SELECT * FROM public.log WHERE fecharegistro >= date_trunc('MONTH', current_date - INTERVAL '2 MONTH')
        AND fecharegistro < date_trunc('MONTH', current_date);)
        TO STDOUT WITH CSV HEADER DELIMITER '|'
    """

    with open(PG_FILENAME, 'w') as f:
        cur.copy_expert(query_export, f)

    cur.close()
    conn.close()

except Exception as e:
    print("Error durante la exportacion desde PostgreSQL:", e)

# Enviar full backup y archivo CSV al COS

APIKEY = os.environ.get("APIKEY")
ENDPOINT = os.environ.get("ENDPOINT")
SERVICE_INSTANCE_ID = os.environ.get("SERVICE_INSTANCE_ID")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
OBJECT_NAME = f"backup_{PG_DATABASE}_{FECHAYHORA}.csv"
BACKUP_OBJECT_NAME = f"fullbackup_{PG_DATABASE}_{FECHAYHORA}.backup"

# Configuracion del cliente para IBM COS

try:

    cos = ibm_boto3.resource("s3",
        ibm_api_key_id=APIKEY,
        ibm_service_instance_id=SERVICE_INSTANCE_ID,
        config=Config(signature_version="oauth"),
        endpoint_url=ENDPOINT
    )

    with open(PG_BACKUP_FILENAME, "rb") as file_data:
        cos.Object(BUCKET_NAME, BACKUP_OBJECT_NAME).upload_fileobj(file_data)

    with open(PG_FILENAME, "rb") as file_data:
        cos.Object(BUCKET_NAME, OBJECT_NAME).upload_fileobj(file_data)

except Exception as e:
    print("Un error ocurrió:", e)

del os.environ["PGPASSWORD"]