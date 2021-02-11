import os
from dotenv import load_dotenv


settings_path = os.path.dirname(__file__)

load_dotenv(f"{settings_path}/.env.example")

db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")
db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASS")
db_name = os.environ.get("DB_NAME")

kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")

kafka_ssl_ca_location = f"{settings_path}/{os.environ['KAFKA_CA_FILE']}"
kafka_ssl_certificate_location = f"{settings_path}/{os.environ['KAFKA_CERTIFICATE_FILE']}"
kafka_ssl_key_location = f"{settings_path}/{os.environ['KAFKA_KEY_FILE']}"

if not os.path.isfile(kafka_ssl_ca_location):
    kafka_ssl_ca_location = None
if not os.path.isfile(kafka_ssl_certificate_location):
    kafka_ssl_certificate_location = None
if not os.path.isfile(kafka_ssl_key_location):
    kafka_ssl_key_location = None
