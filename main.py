# main.py

from kafka_producer import produce_sensor_data
from aggregating import aggregate_sensor_data
from kafka_alert_consumer import consume_alerts
import threading
import time

def start_producer():
    print("Starting sensor data producer...")
    # Выполняем 5 отправок данных, а затем завершаем работу
    for _ in range(5):
        produce_sensor_data()
        time.sleep(1)  # Задержка между отправками данных

def start_aggregator():
    print("Starting data aggregation...")
    aggregate_sensor_data()

def start_alert_consumer():
    print("Starting alert consumer...")
    consume_alerts()

if __name__ == "__main__":
    # Запускаем потоки
    producer_thread = threading.Thread(target=start_producer)
    aggregation_thread = threading.Thread(target=start_aggregator)
    alert_consumer_thread = threading.Thread(target=start_alert_consumer)
    
    producer_thread.start()
    aggregation_thread.start()
    alert_consumer_thread.start()
    
    # Дожидаемся завершения всех потоков
    producer_thread.join()  # Ждем завершения работы продюсера
    aggregation_thread.join()  # Ждем завершения работы агрегатора
    alert_consumer_thread.join()  # Ждем завершения работы потребителя алертов
    
    print("All tasks completed. Exiting the program.")