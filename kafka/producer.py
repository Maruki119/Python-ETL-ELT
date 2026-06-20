from kafka import KafkaProducer
from datetime import datetime, timezone
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

event = {
    "event_type": "user_registered",
    "user_id": 1,
    "username": "Maruki119",
    "created_at": datetime.now(timezone.utc).isoformat(),
}

future = producer.send("quickstart-events", value=event)

result = future.get(timeout=10)
print("Message sent successfully")
print("Topic:", result.topic)
print("Partition:", result.partition)
print("Offset:", result.offset)

producer.flush()
producer.close()