""" 
Source code from Udacity Streaming Nanodegree 
part 2 Kafka Lesson 2.33
adapted to project
"""
import asyncio
from confluent_kafka import Consumer

TOPIC_NAME = "sf.police.calls-for-service.v1"
BROKER_URL = "PLAINTEXT://localhost:9092"

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])

    while True:
        message = c.poll(timeout=1.0)

        if message is None:
           print('no messages received by consumer')
        elif message.error() is not None:
           print(f"{message.error}")
        else:
           print(f"consumed message {message.key()}: {message.value().decode('utf-8')}")

        await asyncio.sleep(0.01)

async def consume_async(topic_name):
    """Runs Consumer tasks"""
    t2 = asyncio.create_task(consume(topic_name))
    await t2

def main():
    try:
        asyncio.run(consume_async(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()