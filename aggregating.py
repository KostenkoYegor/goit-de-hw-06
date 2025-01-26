from kafka_config import create_consumer
import pandas as pd
from datetime import datetime, timedelta

def aggregate_sensor_data(window_duration=60, sliding_interval=30, watermark_duration=10):
    consumer = create_consumer('yk_building_sensors')
    data_buffer = []
    window_start = None
    
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
                    
                    # Reset window
                    window_start = timestamp
                    data_buffer = []
    except KeyboardInterrupt:
        print("Aggregation stopped.")
    finally:
        consumer.close()

aggregate_sensor_data()
