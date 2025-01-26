# kafka_producer.py

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
    """Отправка данных в Kafka топик."""
    producer = None
    try:
        producer = create_producer()
        print("Producer connected successfully.")
        
        topic = 'yk_building_sensors'
        
        while True:
            data = generate_sensor_data()
            try:
                producer.send(topic, value=data)
                print(f"Sent data: {data}")
            except Exception as e:
                print(f"Error while sending data: {e}")
            time.sleep(1)
    
    except KeyboardInterrupt:
        print("Producer stopped.")
    except Exception as e:
        # Обработка ошибок при создании producer или отправке данных
        print(f"Error while creating producer or sending data: {e}")
    finally:
        # Закрытие producer
        if producer:
            producer.close()
            print("Producer closed.")

produce_sensor_data()
