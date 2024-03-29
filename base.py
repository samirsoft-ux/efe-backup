from datetime import datetime, timezone, timedelta
import calendar
import psycopg2
import ibm_boto3
from ibm_botocore.client import Config
import os
import subprocess
import zoneinfo  # Nueva biblioteca en Python 3.9+

# Establecer la zona horaria de Lima, Perú
timezone_lima = zoneinfo.ZoneInfo("America/Lima")

# Conectar a PostgreSQL
PG_HOST = os.environ.get("PG_HOST")
PG_PORT = int(os.environ.get("PG_PORT", 31174))
PG_DATABASE = os.environ.get("PG_DATABASE")
PG_USER = os.environ.get("PG_USER")
PGPASSWORD = os.environ.get("PGPASSWORD")

# Configuración del cliente para IBM COS
cos = ibm_boto3.resource("s3",
    ibm_api_key_id=os.environ.get("APIKEY"),
    ibm_service_instance_id=os.environ.get("SERVICE_INSTANCE_ID"),
    config=Config(signature_version="oauth"),
    endpoint_url=os.environ.get("ENDPOINT")
)

# Enviar full backup al COS
try:
    # Obtener la fecha y hora actual
    ahora = datetime.now(timezone_lima)
    ultimo_dia_del_mes = calendar.monthrange(ahora.year, ahora.month)[1]

    # Determinar el prefijo del nombre del archivo basado en la fecha
    if ahora.weekday() == 6:  # 6 es domingo
        prefijo_nombre_archivo = "fullsemanal_"
    elif ahora.day == ultimo_dia_del_mes:
        prefijo_nombre_archivo = "fullmensual_"
    else:
        prefijo_nombre_archivo = "fullbackup_"

    # Preparar nombres de archivos para el backup
    FECHAYHORA = ahora.strftime('%Y-%m-%d-%H-%M-%S')
    PG_BACKUP_FILENAME = f"./{prefijo_nombre_archivo}{os.environ.get('PG_DATABASE')}_{FECHAYHORA}.backup"
    BACKUP_OBJECT_NAME = f"{prefijo_nombre_archivo}{os.environ.get('PG_DATABASE')}_{FECHAYHORA}.backup"

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
    os.environ["PGPASSWORD"] = PGPASSWORD
    subprocess.run(command, check=True)
    print("Backup completo realizado con éxito")

    # Subir el backup
    with open(PG_BACKUP_FILENAME, "rb") as file_data:
        cos.Object(os.environ.get("BUCKET_NAME"), BACKUP_OBJECT_NAME).upload_fileobj(file_data)

    print(f"Backup subido con éxito a IBM COS")
except Exception as e:
    print("Un error ocurrió durante la subida a IBM COS ", e)

# Limpiar variable de entorno
del os.environ["PGPASSWORD"]