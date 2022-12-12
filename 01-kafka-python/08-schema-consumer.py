from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient

import asyncio
from dotenv import dotenv_values


config = dotenv_values(".env")


def print_assignment(consumer, partitions):
    print('Assignment:', partitions)


async def consume(consumer, topic_name):
    # sleep wait for producer send data
    await asyncio.sleep(2.5)

    # poll for message
    consumer.subscribe([topic_name], 
                    on_assign=print_assignment) 

    while True:
        message = consumer.poll(1)
        if message is None:
            # print("No message received from producer")
            pass
        elif message.error() is not None:
            print(f"Error message {message.error()}")
        else:
            try:
                print(f"Consumed message {message.key()}: {message.value()} - Partition {message.partition()} - Offset {message.offset()}")
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1)


async def create_consumers(consumer, topic_name):
    consumer_task = asyncio.create_task(consume(consumer, topic_name))
    await consumer_task


if __name__ == "__main__":
    # create cached schema registry client
    schema_registry = CachedSchemaRegistryClient(config["SCHEMA_REGISTRY_URL"])
    
    # init Avro consumer schema registry to consumer
    consumer_config = {
        "bootstrap.servers": config["BROKER_URL"],
        "group.id": "python-app-02",
        "auto.offset.reset": "earliest"
    }
    consumer = AvroConsumer(consumer_config, 
                            schema_registry=schema_registry)

    # consume message
    try: 
        asyncio.run(create_consumers(consumer, config["TOPIC_NAME"]))
    except KeyboardInterrupt as e:
        print("Keyboard interrupt")
    finally:
        consumer.close()