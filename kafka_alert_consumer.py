#kafka_alert_consumer.py

from kafka_config import create_consumer, create_producer
import csv
from datetime import timedelta

# Load alert conditions from CSV
def load_alert_conditions(file_path="alerts_conditions.csv"):
    conditions = []
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            conditions.append(row)
    return conditions

# Determine if the data triggers an alert
def check_alert_conditions(data, alert_conditions):
    for condition in alert_conditions:
        temp_min = float(condition["temp_min"])
        temp_max = float(condition["temp_max"])
        humidity_min = float(condition["humidity_min"])
        humidity_max = float(condition["humidity_max"])
        
        if (temp_min != -999 and data["temperature"] < temp_min) or (temp_max != -999 and data["temperature"] > temp_max):
            return {"alert": condition["message"], "code": condition["code"]}
        elif (humidity_min != -999 and data["humidity"] < humidity_min) or (humidity_max != -999 and data["humidity"] > humidity_max):
            return {"alert": condition["message"], "code": condition["code"]}
    return None

def consume_alerts():
    consumer = create_consumer('yk_building_sensors')
    producer = create_producer()
    alert_conditions = load_alert_conditions()
    alert_topic = 'alert-Kafka-topic'
    
    try:
        for message in consumer:
            data = message.value
            alert = check_alert_conditions(data, alert_conditions)
            
            if alert:
                alert_data = {
                    "window": {
                        "start": message.timestamp,
                        "end": message.timestamp + timedelta(minutes=1)
                    },
                    "t_avg": data["temperature"],
                    "h_avg": data["humidity"],
                    "code": alert["code"],
                    "message": alert["alert"],
                    "timestamp": message.timestamp
                }
                producer.send(alert_topic, value=alert_data)
                print(f"Alert sent: {alert_data}")
    except KeyboardInterrupt:
        print("Alert consumer stopped.")
    finally:
        consumer.close()
        producer.close()

consume_alerts()
