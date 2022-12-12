# Please complete the TODO items in the code.

from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Producer
from faker import Faker

from datetime import datetime
import time
import sys

from dotenv import dotenv_values

faker = Faker()

config = dotenv_values(".env")

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


def produce(producer, topic_name, synchronous):
    """Produces data synchronously into the Kafka Topic"""
    

    # TODO: Write a synchronous production loop.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    curr_iteration = 0
    start_time = datetime.utcnow()

    while True:
        # TODO: Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        producer.produce(topic_name, Purchase().serialize(), callback=delivery_callback)
        

        if synchronous:
            producer.flush()
        else:
            producer.poll(0)

        if curr_iteration % 1000 == 0 and curr_iteration != 0:
            duration = datetime.utcnow() - start_time
            sys.stdout.write(f"SEND {curr_iteration} messages - duration {duration.seconds}")
            time.sleep(50)
    
        
        curr_iteration += 1

def delivery_callback(err, msg):
    if err:
        sys.stdout.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stdout.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

if __name__ == "__main__":
    p = Producer({"bootstrap.servers": config["BROKER_URL"]})
    try:
        produce(p, config["TOPIC_NAME"], True)
    except KeyboardInterrupt as e:
        p.flush()
        print("shutting down")
