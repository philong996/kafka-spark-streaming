from confluent_kafka import Consumer, OFFSET_BEGINNING
import asyncio
from dotenv import dotenv_values

config = dotenv_values(".env")

async def consume(consumer, topic_name):
    # sleep wait for producer send data
    await asyncio.sleep(2.5)

    # poll for message
    consumer.subscribe([topic_name], on_assign=print_assignment) 

    while True:
        messages = consumer.consume(10, 1)

        for message in messages:
            if message is None:
                print("No message received from producer")
            elif message.error() is not None:
                print(f"Error message {message.error()}")
            else:
                print(f"Consumed message from topic {message.topic()} - {message.key()}: {message.value()} - Partition {message.partition()} - Offset {message.offset()}")
            await asyncio.sleep(1)

def reset_offset(consumer, partitions):
    """Callback for when topic assignment takes place"""
    # TODO: Set the partition offset to the beginning on every boot.
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    # TODO: Assign the consumer the partitions
    consumer.assign(partitions)

def print_assignment(consumer, partitions):
    print('Assignment:', partitions)


async def create_consumers(consumer, topic_name):
    consumer_task = asyncio.create_task(consume(consumer, config["TOPIC_NAME"]))
    await consumer_task

if __name__ == "__main__":
    # init consumer
    consumer = Consumer({
        "bootstrap.servers": config["BROKER_URL"],
        "group.id": "python-app-01",
        "auto.offset.reset": "earliest"
    })

    # consume message
    try: 
        asyncio.run(create_consumers(consumer, config["TOPIC_NAME"]))
    except KeyboardInterrupt as e:
        print("Keyboard interrupt")
    finally:
        consumer.close()