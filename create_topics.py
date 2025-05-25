
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

my_name = "oleg"
topics = [
    f'{my_name}_building_sensors',
    f'{my_name}_temperature_alerts',
    f'{my_name}_humidity_alerts'
]

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

new_topics = [NewTopic(name=topic, num_partitions=1, replication_factor=1) for topic in topics]

try:
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print(f"Topics {topics} created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

print("Current topics:", admin_client.list_topics())
admin_client.close()
