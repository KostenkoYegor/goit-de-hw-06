# aggregating.py

from kafka_config import KAFKA_CONFIG, create_consumer  
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
import sys

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
    security_protocol=KAFKA_CONFIG['security_protocol'],
    sasl_mechanism=KAFKA_CONFIG['sasl_mechanism'],
    sasl_plain_username=KAFKA_CONFIG['sasl_plain_username'],
    sasl_plain_password=KAFKA_CONFIG['sasl_plain_password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_alert(window_start, window_end, t_avg, h_avg, code, message):
    alert_data = {
        'window': {
            'start': window_start.isoformat(),
            'end': window_end.isoformat()
        },
        't_avg': t_avg,
        'h_avg': h_avg,
        'code': code,
        'message': message,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    }
    producer.send('alert-Kafka-topic', value=alert_data)
    print(f"Alert sent: {alert_data}")

def aggregate_sensor_data(window_duration=60, sliding_interval=30, watermark_duration=10):
    consumer = create_consumer('yk_building_sensors')
    data_buffer = []
    window_start = None
    
    alert_conditions = {
        'min_temperature': 18,
        'max_temperature': 30,
        'min_humidity': 40,
        'max_humidity': 70,
        'code': '104',
        'message': 'Alert: Conditions outside of the safe range'
    }

    print("Aggregation process started.")
    
    try:
        while True:
            for message in consumer:
                data = message.value
                timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S")
                data_buffer.append({"timestamp": timestamp, "temperature": data["temperature"], "humidity": data["humidity"]})
                print(f"Data received: {data}")
                
                data_buffer = [entry for entry in data_buffer if entry["timestamp"] > timestamp - timedelta(seconds=window_duration)]
                print(f"Buffer after cleaning: {data_buffer}")
                
                if window_start is None:
                    window_start = timestamp
                    print(f"Window start set to: {window_start}")
                
                if timestamp - window_start >= timedelta(seconds=window_duration):
                    df = pd.DataFrame(data_buffer)
                    avg_temp = df["temperature"].mean()
                    avg_humidity = df["humidity"].mean()
                    print(f"Aggregated Data - Temp: {avg_temp}, Humidity: {avg_humidity}")
                    
                    if (alert_conditions['min_temperature'] <= avg_temp <= alert_conditions['max_temperature'] and
                        alert_conditions['min_humidity'] <= avg_humidity <= alert_conditions['max_humidity']):
                        window_end = timestamp
                        print(f"Conditions met for alert. Sending alert...")
                        send_alert(window_start, window_end, avg_temp, avg_humidity, alert_conditions['code'], alert_conditions['message'])
                    
                    window_start = timestamp
                    data_buffer = []
                    print(f"Window reset. New window start: {window_start}")
                    
    except KeyboardInterrupt:
        print("Aggregation stopped.")
    finally:
        consumer.close()
        print("Consumer closed.")
        sys.exit(0)  # Ensure the program exits gracefully after interruption
