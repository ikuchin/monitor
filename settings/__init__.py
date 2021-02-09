import os
from dotenv import load_dotenv


settings_path = os.path.dirname(__file__)

load_dotenv(f"{settings_path}/.env.aiven")
# load_dotenv(f"{settings_path}/.env.local")

db_host = os.environ.get("DB_HOST")
db_port = os.environ.get("DB_PORT")
db_user = os.environ.get("DB_USER")
db_pass = os.environ.get("DB_PASS")
db_name = os.environ.get("DB_NAME")

kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
kafka_ssl_ca_location = settings_path + "/" + os.environ.get("KAFKA_CA_FILE", "ca.pem")
kafka_ssl_certificate_location = settings_path + "/" + os.environ.get("KAFKA_CERTIFICATE_FILE", "service.cert")
kafka_ssl_key_location = settings_path + "/" + os.environ.get("KAFKA_KEY_FILE", "service.key")
