from kafka_config import create_producer
import time
import random

def generate_sensor_data():
    return {
        "id": random.randint(1, 100),
        "temperature": random.uniform(18, 35),
        "humidity": random.uniform(20, 80),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }

def produce_sensor_data():
    producer = create_producer()
    topic = 'yk_building_sensors'
    
    try:
        while True:
            data = generate_sensor_data()
            producer.send(topic, value=data)
            print(f"Sent data: {data}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()

produce_sensor_data()
