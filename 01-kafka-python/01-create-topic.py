from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import dotenv_values

config = dotenv_values(".env")


def create_topic(admin_client, topic_name):
    new_topic = NewTopic(
        topic= topic_name,
        num_partitions= 3,
        replication_factor= 2,
        config= {
            "cleanup.policy": "delete",
            "compression.type": "lz4",
            "delete.retention.ms": 2
        }
    )

    # create a new topic
    new_topics = [new_topic]
    futures = admin_client.create_topics(new_topics)
    
    for topic, future in futures.items():
        try:
            future.result()
            print(f"created a topic: {topic}")
        except Exception as e:
            print(f"failed to create a topic {e}")


if __name__ == "__main__":
    # create admin client
    admin_client = AdminClient({"bootstrap.servers": config["BROKER_URL"]})

    # configure a new topic
    topic_name = config["TOPIC_NAME"]
    create_topic(admin_client, topic_name)

