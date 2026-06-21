import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "bankbranch"


admin_client = KafkaAdminClient(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    client_id="test"
)

topic_list = [
    NewTopic(
        name=TOPIC_NAME,
        num_partitions=2,
        replication_factor=1
    )
]

try:
    admin_client.create_topics(new_topics=topic_list)
    print(f"Created topic: {TOPIC_NAME}")
except TopicAlreadyExistsError:
    print(f"Topic already exists: {TOPIC_NAME}")

configs = admin_client.describe_configs(
    config_resources=[
        ConfigResource(ConfigResourceType.TOPIC, TOPIC_NAME)
    ]
)

print("Topic config:")
print(configs)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

producer.send(TOPIC_NAME, {"atmid": 1, "transid": 100})
producer.send(TOPIC_NAME, {"atmid": 2, "transid": 101})

producer.flush()
producer.close()

print("Messages sent.")

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    consumer_timeout_ms=5000
)

print("Consuming messages:")

for msg in consumer:
    print(msg.value.decode("utf-8"))

consumer.close()
admin_client.close()