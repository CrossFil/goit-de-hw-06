
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from configs import kafka_config

SENSOR_ID = random.randint(1000, 9999)
TOPIC = 'oleg_building_sensors'

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_sensor_data():
    return {
        "sensor_id": SENSOR_ID,
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2)
    }

while True:
    data = generate_sensor_data()
    producer.send(TOPIC, value=data)
    print(f"Sent: {data}")
    time.sleep(2)
