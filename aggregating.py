# aggregating.py

from kafka_config import create_consumer
import pandas as pd
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='your_bootstrap_servers',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='your_username',
    sasl_plain_password='your_password',
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
    
    # Example alert conditions
    alert_conditions = {
        'min_temperature': 18,
        'max_temperature': 30,
        'min_humidity': 40,
        'max_humidity': 70,
        'code': '104',
        'message': 'Alert: Conditions outside of the safe range'
    }
    
    try:
        while True:
            for message in consumer:
                data = message.value
                timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S")
                
                # Add data to buffer
                data_buffer.append({"timestamp": timestamp, "temperature": data["temperature"], "humidity": data["humidity"]})
                
                # Remove old data based on sliding window
                data_buffer = [entry for entry in data_buffer if entry["timestamp"] > timestamp - timedelta(seconds=window_duration)]
                
                if window_start is None:
                    window_start = timestamp
                
                if timestamp - window_start >= timedelta(seconds=window_duration):
                    df = pd.DataFrame(data_buffer)
                    avg_temp = df["temperature"].mean()
                    avg_humidity = df["humidity"].mean()
                    print(f"Aggregated Data - Temp: {avg_temp}, Humidity: {avg_humidity}")
                    
                    # Check for alert conditions
                    if (alert_conditions['min_temperature'] <= avg_temp <= alert_conditions['max_temperature'] and
                        alert_conditions['min_humidity'] <= avg_humidity <= alert_conditions['max_humidity']):
                        window_end = timestamp
                        send_alert(window_start, window_end, avg_temp, avg_humidity, alert_conditions['code'], alert_conditions['message'])
                    
                    # Reset window
                    window_start = timestamp
                    data_buffer = []
    except KeyboardInterrupt:
        print("Aggregation stopped.")
    finally:
        consumer.close()

aggregate_sensor_data()
