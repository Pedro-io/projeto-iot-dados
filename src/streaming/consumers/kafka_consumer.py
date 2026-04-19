import json
from typing import Any, Iterator

from kafka import KafkaConsumer

from .base import MessageConsumer


class KafkaMessageConsumer(MessageConsumer):

    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )

    def consume(self) -> Iterator[Any]:
        for msg in self.consumer:
            yield msg

    def commit(self) -> None:
        self.consumer.commit()

    def close(self) -> None:
        self.consumer.close()