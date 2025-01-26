# main.py

from kafka_producer import produce_sensor_data
from aggregating import aggregate_sensor_data
from kafka_alert_consumer import consume_alerts

if __name__ == "__main__":
    # Run producer, aggregation and alert consumer in parallel
    import threading
    
    producer_thread = threading.Thread(target=produce_sensor_data)
    aggregation_thread = threading.Thread(target=aggregate_sensor_data)
    alert_consumer_thread = threading.Thread(target=consume_alerts)
    
    producer_thread.start()
    aggregation_thread.start()
    alert_consumer_thread.start()
    
    producer_thread.join()
    aggregation_thread.join()
    alert_consumer_thread.join()