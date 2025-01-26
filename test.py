from kafka import KafkaConsumer

KAFKA_CONFIG = {
    'bootstrap_servers': "77.81.230.104:9092",
    'security_protocol': "SASL_PLAINTEXT",
    'sasl_mechanism': "PLAIN",
    'sasl_plain_username': "admin",
    'sasl_plain_password': "VawEzo1ikLtrA8Ug8THa"
}

# Consumer example
consumer = KafkaConsumer(
    'your_topic',  # Замените на ваш актуальный топик
    **KAFKA_CONFIG,
    group_id='your_group_id',
    auto_offset_reset='earliest'  # Для получения всех сообщений с начала
)

# Прочитаем и выведем все сообщения
for message in consumer:
    print(f"Received message: {message.value.decode('utf-8')}")
    # После получения одного сообщения можно выйти
    break
