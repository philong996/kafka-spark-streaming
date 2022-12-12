from confluent_kafka import Producer

from dataclasses import dataclass, field
import json
from faker import Faker
import random

import asyncio
from datetime import datetime
from dotenv import dotenv_values

config = dotenv_values(".env")
faker = Faker()

@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # TODO: Serializer the Purchase object
        #       See: https://docs.python.org/3/library/json.html#json.dumps
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount
            }
        )

async def produce(producer, topic_name):
    start_time = datetime.utcnow()
    curr_iteration = 0

    # submit iteration message
    while True:
        producer.produce(topic_name, Purchase().serialize())  
        
        if curr_iteration % 100 == 0:
            elapsed = datetime.utcnow() - start_time
            print(f"{curr_iteration} messages send in {elapsed.seconds}")
            await asyncio.sleep(10)

        curr_iteration += 1

        producer.poll(0)

async def create_producers(producer, topic_name):
    produce_task = asyncio.create_task(produce(producer, config["TOPIC_NAME"]))
    await produce_task

if __name__ == "__main__":
    # init producer
    producer = Producer({
        "bootstrap.servers": config["BROKER_URL"],
        # "compression.type": "lz4",
        "linger.ms": "10000",
        "batch.num.messages": "10000"
        # "queue.buffering.max.messages": "10000"
    })

    try: 
        asyncio.run(create_producers(producer, config["TOPIC_NAME"]))
    except KeyboardInterrupt as e:
        print("Keyboard interrupt")
    finally:
        producer.flush()
        print("Shutting down")
    


