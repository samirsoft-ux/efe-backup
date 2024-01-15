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
    os.environ["PGPASSWORD"] = PGPASSWORD
    subprocess.run(command, check=True)
    print("Backup completo realizado con éxito")
except Exception as e:
    print("Error durante el backup de PostgreSQL:", e)

# Configuración del cliente para IBM COS
cos = ibm_boto3.resource("s3",
    ibm_api_key_id=os.environ.get("APIKEY"),
    ibm_service_instance_id=os.environ.get("SERVICE_INSTANCE_ID"),
    config=Config(signature_version="oauth"),
    endpoint_url=os.environ.get("ENDPOINT")
)

# Obtener el cliente s3 de ibm_boto3
s3_client = ibm_boto3.client('s3',
    ibm_api_key_id=os.environ.get("APIKEY"),
    ibm_service_instance_id=os.environ.get("SERVICE_INSTANCE_ID"),
    config=Config(signature_version='oauth'),
    endpoint_url=os.environ.get("ENDPOINT")
)

# Variables para el archivo de contador en COS
CONTADOR_BUCKET_NAME = os.environ.get("CONTADOR_BUCKET_NAME")
CONTADOR_ARCHIVO = "contador_backup.txt"

# Funciones para manejar el contador en COS
def leer_contador():
    try:
        obj = cos.Object(CONTADOR_BUCKET_NAME, CONTADOR_ARCHIVO).get()
        return int(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        raise Exception("No se pudo leer el archivo de contador en COS: " + str(e))

# Enviar full backup al COS
try:
    # Leer el contador actual
    contador = leer_contador()
    contador += 1

    # Preparar nombres de archivos para el backup
    BACKUP_OBJECT_NAME = f"fullbackup_{PG_DATABASE}_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.backup"

    # Subir el backup
    with open(PG_BACKUP_FILENAME, "rb") as file_data:
        cos.Object(os.environ.get("BUCKET_NAME"), BACKUP_OBJECT_NAME).upload_fileobj(file_data)

    # Definir y aplicar las etiquetas al objeto subido
    tags = {'TagSet': [{'Key': 'Numero', 'Value': str(contador)}]}
    s3_client.put_object_tagging(
        Bucket=os.environ.get("BUCKET_NAME"),
        Key=BACKUP_OBJECT_NAME,
        Tagging=tags
    )

    print(f"Backup {contador} subido con éxito a IBM COS con etiquetas")
except Exception as e:
    print("Un error ocurrió durante la subida a IBM COS:", e)

# Limpiar variable de entorno
del os.environ["PGPASSWORD"]
