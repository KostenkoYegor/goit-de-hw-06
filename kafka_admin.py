# kafka_admin.py

from kafka_config import create_admin_client
from kafka.admin import NewTopic

def create_topics():
    admin_client = create_admin_client()

    topics = [
        NewTopic(name="yk_building_sensors", num_partitions=1, replication_factor=1),
        NewTopic(name="yk_temperature_alerts", num_partitions=1, replication_factor=1),
        NewTopic(name="yk_humidity_alerts", num_partitions=1, replication_factor=1),
    ]

    try:

        existing_topics = set(admin_client.list_topics())
        topics_to_create = [topic for topic in topics if topic.name not in existing_topics]

        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print(f"Created topics: {[topic.name for topic in topics_to_create]}")
        else:
            print("All topics already exist.")
    except Exception as e:
        print(f"Error creating topics: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_topics()
