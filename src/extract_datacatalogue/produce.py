import os
import sys
import time
import uuid
from pathlib import Path
from pprint import pprint
import json
from nuvla.api import Api as Nuvla

from extract_datacatalogue.catalogue.producer import DataProducer

def import_config(filename: str) -> dict:
    with open(f"{filename}") as f:
        data = json.load(f)
    return data

config=import_config("config.json")
print(config)
NUVLA_ENDPOINT = config["NUVLA_ENDPOINT"]
NUVLA_KEY = config["NUVLA_KEY"]
NUVLA_KEY_SECRET = config["NUVLA_KEY_SECRET"]

# MQTT Configuration
#TOPIC = config["MQTT_TOPIC", "test-topic")
#MQTT_BROKER: str = config["MQTT_BROKER", "91.134.104.104")
#MQTT_PORT: int = int(config["MQTT_PORT", 1883))

# S3 Configuration
S3_BUCKET = config["S3_BUCKET"]
S3_CREDENTIAL = config["S3_CREDENTIAL"]
S3_INFRA_SERVICE_ID = config["S3_INFRA_SERVICE_ID"]
print(S3_BUCKET)

def main(file: Path):
    nuvla: Nuvla = Nuvla(endpoint=NUVLA_ENDPOINT, insecure=False, )
    resp = nuvla.login_apikey(NUVLA_KEY, NUVLA_KEY_SECRET)
    print(resp)
    #resp = nuvla.login_password(NUVLA_LOGIN, NUVLA_PASSWORD)

    if not resp or resp.status_code != 201:
        print("Failed to login to Nuvla. Please check your credentials.")
        print(resp.content)
        exit(1)

    producer: DataProducer = DataProducer(
        nuvla=nuvla,
        bucket=S3_BUCKET,
        s3_credential=S3_CREDENTIAL,
        infra_service_id=S3_INFRA_SERVICE_ID
    )
    name = get_ts_file_name(file)
    with file.open(mode='rb') as f:
        producer.produce(
            content=f.read(),
            object_path=name,
            name=name,
            description="Produced by Nuvla Python SDK",
            content_type='application/tensor',
            tags=['example', 'nuvla', 'data-catalogue', 'per', 'tensor'],
        )


def get_ts_file_name(file: Path) -> str:
    stem = file.stem
    suffix = file.suffix
    timestamp = int(time.time())
    return f"{stem}_{timestamp}{suffix}"


if __name__ == "__main__":
    
    if not NUVLA_KEY or not NUVLA_KEY_SECRET:
        print("Please set NUVLA_KEY and NUVLA_KEY_SECRET environment variables.")
        exit(1)

    if len(sys.argv) < 2:
        print("Usage: python produce.py <file_path>")
        exit(1)
    file_path = sys.argv[1]
    file_path = Path(file_path).expanduser().resolve()
    if not file_path.is_file():
        print(f"The file {file_path} does not exist.")
        exit(1)

    main(file_path)
