from kafka_config import create_consumer

def consume_sensor_data():
    consumer = create_consumer('yk_building_sensors')
    
    try:
        print("Subscribed to 'yk_building_sensors' topic.")
        for message in consumer:
            data = message.value
            print(f"Received data: {data}")
    except KeyboardInterrupt:
        print("Consumer stopped.")
    finally:
        consumer.close()

consume_sensor_data()
