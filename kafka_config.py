from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import json

KAFKA_CONFIG = {
    'bootstrap_servers': "77.81.230.104:9092",
    'security_protocol': "SASL_PLAINTEXT",
    'sasl_mechanism': "PLAIN",
    'sasl_plain_username': "admin",
    'sasl_plain_password': "VawEzo1ikLtrA8Ug8THa"
}

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        security_protocol=KAFKA_CONFIG['security_protocol'],
        sasl_mechanism=KAFKA_CONFIG['sasl_mechanism'],
        sasl_plain_username=KAFKA_CONFIG['sasl_plain_username'],
        sasl_plain_password=KAFKA_CONFIG['sasl_plain_password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def create_consumer(topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        security_protocol=KAFKA_CONFIG['security_protocol'],
        sasl_mechanism=KAFKA_CONFIG['sasl_mechanism'],
        sasl_plain_username=KAFKA_CONFIG['sasl_plain_username'],
        sasl_plain_password=KAFKA_CONFIG['sasl_plain_password'],
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def create_admin_client():
    return KafkaAdminClient(
        bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
        client_id="admin",
        security_protocol=KAFKA_CONFIG['security_protocol'],
        sasl_mechanism=KAFKA_CONFIG['sasl_mechanism'],
        sasl_plain_username=KAFKA_CONFIG['sasl_plain_username'],
        sasl_plain_password=KAFKA_CONFIG['sasl_plain_password']
    )
