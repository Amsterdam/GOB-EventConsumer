import os

import pika

MESSAGE_BROKER = os.getenv("MESSAGE_BROKER_ADDRESS", "localhost")
MESSAGE_BROKER_PORT = os.getenv("MESSAGE_BROKER_PORT", 5672)
MESSAGE_BROKER_VHOST = os.getenv("MESSAGE_BROKER_VHOST", "gob")
MESSAGE_BROKER_USER = os.getenv("MESSAGE_BROKER_USERNAME", "guest")
MESSAGE_BROKER_PASSWORD = os.getenv("MESSAGE_BROKER_PASSWORD", "guest")

CONNECTION_PARAMS = pika.ConnectionParameters(
    host=MESSAGE_BROKER,
    virtual_host=MESSAGE_BROKER_VHOST,
    port=MESSAGE_BROKER_PORT,
    credentials=pika.PlainCredentials(username=MESSAGE_BROKER_USER, password=MESSAGE_BROKER_PASSWORD),
    heartbeat=1200,
    blocked_connection_timeout=600,
)

EVENTS_EXCHANGE = "gob.events"

CATALOGS = os.getenv("LISTEN_TO_CATALOGS", "").split(",")
DATABASE_URL = os.getenv("DATABASE_URL", "")
SCHEMA_URL = os.getenv("SCHEMA_URL", "")

INSTANCE_CNT = int(os.getenv("INSTANCE_CNT", 1))
INSTANCE_IDX = int(os.getenv("INSTANCE_IDX", 0))

assert INSTANCE_IDX < INSTANCE_CNT, "INSTANCE_IDX must be less than INSTANCE_CNT"
