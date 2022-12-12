from confluent_kafka import avro

from dataclasses import dataclass, field, asdict
from faker import Faker
from fastavro import parse_schema, writer

import io
import json
import random

faker = Faker()

@dataclass
class LineItem:
    description: str = field(default_factory=faker.bs)
    amount: int = field(default_factory=lambda: random.randint(100, 20000))

    @classmethod
    def line_item(self):
        return [LineItem() for _ in range(random.randint(1,10))]

@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))
    line_items: list = field(default_factory=LineItem.line_item)
    
    arvo_schema = """{
        "type": "record",
        "name": "purchase",
        "namespace": "longnpp.Purchase.Order",
        "fields": [
            {"name": "username", "type": "string"},
            {"name": "currency", "type": "string"},
            {"name": "amount", "type": "int"},
            {"name": "line_items", "type": {
                "type": "array",
                "items": {
                    "name": "line_items",
                    "type": "record",
                    "fields": [
                        {"type": "int", "name": "amount"},
                        {"type": "string", "name": "description"}
                    ]
                }
            }}
        ]
    }"""

    schema = avro.loads(arvo_schema)


    def schema_serialize(self):
        """Serializes the object in JSON string format"""
        out = io.BytesIO()
        writer(out, Purchase.arvo_schema, [asdict(self)])
        return out.getvalue()


    def serialize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(asdict(self))

    
    def generate_purchase(self):
        return asdict(self)


    def deserialize(self):
        pass

if __name__ == "__main__":

    print(asdict(Purchase()))
    print(Purchase().generate_purchase())