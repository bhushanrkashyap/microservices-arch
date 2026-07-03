import os

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def producer_config():
    return {"bootstrap.servers": BOOTSTRAP_SERVERS}


def consumer_config(group_id, offset_reset="earliest"):
    return {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
    }
