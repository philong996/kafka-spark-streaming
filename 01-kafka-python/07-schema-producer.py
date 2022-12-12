from confluent_kafka import Producer
from confluent_kafka.avro import CachedSchemaRegistryClient, AvroProducer

from src.simulate_data import Purchase

import asyncio
from dotenv import dotenv_values
from datetime import datetime

from dataclasses import asdict

config = dotenv_values(".env")

async def avro_produce(producer, topic_name, schema):
    start_time = datetime.utcnow()
    curr_iteration = 0

    # submit iteration message
    while True:
        producer.produce(
            topic=topic_name, 
            value=Purchase().generate_purchase(),
            value_schema=schema
            )  
        
        await asyncio.sleep(1)
        
        if curr_iteration % 100 == 0:
            elapsed = datetime.utcnow() - start_time
            print(f"{curr_iteration} messages send in {elapsed.seconds}")
            

        curr_iteration += 1

        producer.poll(0)

async def create_producers(producer, topic_name, schema):
    produce_task = asyncio.create_task(avro_produce(producer, topic_name, schema))
    await produce_task


if __name__ == "__main__":
    # create cached schema registry client
    schema_registry = CachedSchemaRegistryClient(config["SCHEMA_REGISTRY_URL"])

    # init Arvo producer and 
    producer_config = {
        "bootstrap.servers": config["BROKER_URL"],
        # "compression.type": "lz4",
        "linger.ms": "10000",
        "batch.num.messages": "10000",
        # "queue.buffering.max.messages": "10000",
        #  add registry to producer
    }
    producer = AvroProducer(producer_config, 
                            schema_registry= schema_registry)

    try: 
        asyncio.run(create_producers(producer, config["TOPIC_NAME"], Purchase.schema))
    except KeyboardInterrupt as e:
        print("Keyboard interrupt")
    finally:
        producer.flush()
        print("Shutting down")