from kafka import KafkaConsumer
import json

# Set up the Kafka consumer
consumer = KafkaConsumer(
    'news_topic',  # Topic name to consume messages from
    bootstrap_servers='localhost:9092',  # Replace with your Kafka server address
    auto_offset_reset='earliest',  # Start from the beginning of the topic if no offset is committed
    enable_auto_commit=True,  # Automatically commit the offset after consuming the message
    group_id='news_group',  # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize the JSON data
)

print("Kafka consumer is ready and listening for messages...")

# Read messages from the Kafka topic
for message in consumer:
    article = message.value
    print(f"Received article from Kafka: {article.get('title')}")