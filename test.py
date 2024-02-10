from datetime import datetime, timezone, timedelta
import os
import calendar
import psycopg2
import ibm_boto3
from ibm_botocore.client import Config
import subprocess
import zoneinfo  # Nueva biblioteca en Python 3.9+
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_secrets_manager_sdk.secrets_manager_v2 import SecretsManagerV2
import json

# Establecer la zona horaria de Lima, Perú
timezone_lima = zoneinfo.ZoneInfo("America/Lima")

# Configuración de IBM Secret Manager
secret_api_key = os.environ.get("SECRET_IBM_API_KEY")
authenticator = IAMAuthenticator(secret_api_key)
secrets_manager = SecretsManagerV2(authenticator=authenticator)
secrets_manager.set_service_url('https://65e7ac31-7d3d-4c5f-9545-f848e11f8a26.private.us-south.secrets-manager.appdomain.cloud')

def obtener_secreto(secret_id):
    response = secrets_manager.get_secret(id=secret_id)
    secret_data = response.get_result()
    if 'data' in secret_data:
        secret_values = secret_data['data']
        return secret_values
    else:
        print("La estructura del secreto no es como se esperaba.")
        return {}

# Obtener los secretos
secret_id = os.environ.get("SECRET_ID_PORTAL")
secretos = obtener_secreto(secret_id)

# Asignar valores a las variables desde los secretos obtenidos
PG_HOST = secretos['PG_HOST']
PG_PORT = secretos['PG_PORT']
PG_DATABASE = secretos['PG_DATABASE']
PG_USER = secretos['PG_USER']
PGPASSWORD = secretos['PGPASSWORD']

APIKEY = secretos['APIKEY']
SERVICE_INSTANCE_ID = secretos['SERVICE_INSTANCE_ID']
ENDPOINT = secretos['ENDPOINT']

BUCKET_NAME = secretos['BUCKET_NAME']

# A partir de aquí, el resto del script permanece igual, pero ahora utilizando las variables asignadas desde los secretos
# Por ejemplo, la configuración de IBM COS utilizaría APIKEY, SERVICE_INSTANCE_ID, y ENDPOINT obtenidos de los secretos
# Configuración del cliente para IBM COS
cos = ibm_boto3.resource("s3",
    ibm_api_key_id=APIKEY,
    ibm_service_instance_id=SERVICE_INSTANCE_ID,
    config=Config(signature_version="oauth"),
    endpoint_url=ENDPOINT
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
    PG_BACKUP_FILENAME = f"./{prefijo_nombre_archivo}{PG_DATABASE}_{FECHAYHORA}.backup"
    BACKUP_OBJECT_NAME = f"{prefijo_nombre_archivo}{PG_DATABASE}_{FECHAYHORA}.backup"

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
        cos.Object(BUCKET_NAME, BACKUP_OBJECT_NAME).upload_fileobj(file_data)

    print(f"Backup subido con éxito a IBM COS")
except Exception as e:
    print("Un error ocurrió durante la subida a IBM COS ", e)

# Limpiar variable de entorno
del PGPASSWORD