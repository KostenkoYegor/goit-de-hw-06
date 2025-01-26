#test.py

from kafka import KafkaConsumer

KAFKA_CONFIG = {
    'bootstrap_servers': "77.81.230.104:9092",
    'security_protocol': "SASL_PLAINTEXT",
    'sasl_mechanism': "PLAIN",
    'sasl_plain_username': "admin",
    'sasl_plain_password': "VawEzo1ikLtrA8Ug8THa"
}

topics = [
    'yk_building_sensors',
    'yk_temperature_alerts',
    'yk_humidity_alerts'
]
consumer = KafkaConsumer(
    *topics,  
    **KAFKA_CONFIG,
    group_id='your_group_id',
    auto_offset_reset='earliest'  
)

for message in consumer:
    print(f"Received message from topic '{message.topic}': {message.value.decode('utf-8')}")
   
    break
