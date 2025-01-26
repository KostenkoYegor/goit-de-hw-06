# main.py
from kafka_config import create_producer, create_consumer
import threading

def send_sensor_data():
    producer = create_producer()

    # Example of sending sensor data
    try:
        producer.send('yk_building_sensors', value={'sensor_id': 123, 'temperature': 25, 'humidity': 60})
        producer.send('yk_temperature_alerts', value={'sensor_id': 123, 'alert': 'Temperature too high'})
        producer.send('yk_humidity_alerts', value={'sensor_id': 123, 'alert': 'Humidity out of range'})
        producer.flush()  # Wait for the messages to be sent
        print("Data sent successfully.")
    except Exception as e:
        print(f"Error sending data: {e}")
    finally:
        producer.close()

def consume_alerts():
    consumer = create_consumer('yk_temperature_alerts')  # Example for consuming alerts

    for message in consumer:
        print(f"Received message from topic '{message.topic}': {message.value}")
        break  # Stop after receiving the first message

def aggregate_sensor_data():
    # Logic for aggregating data, if necessary
    print("Aggregating sensor data...")

if __name__ == "__main__":
    # Start producer, aggregation, and consumer in separate threads
    producer_thread = threading.Thread(target=send_sensor_data)
    aggregation_thread = threading.Thread(target=aggregate_sensor_data)
    alert_consumer_thread = threading.Thread(target=consume_alerts)

    producer_thread.start()
    aggregation_thread.start()
    alert_consumer_thread.start()

    producer_thread.join()
    aggregation_thread.join()
    alert_consumer_thread.join()
