# kafka_admin.py

from kafka_config import create_admin_client, NewTopic

def create_topics():
    admin_client = create_admin_client()
    
    topics = [
        NewTopic(name="yk_building_sensors", num_partitions=1, replication_factor=1),
        NewTopic(name="yk_temperature_alerts", num_partitions=1, replication_factor=1),
        NewTopic(name="yk_humidity_alerts", num_partitions=1, replication_factor=1),
    ]
    
    try:
        existing_topics = admin_client.list_topics()
        topics_to_create = [topic for topic in topics if topic.name not in existing_topics]
        
        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
            print("Topics created successfully.")
        else:
            print("Topics already exist.")
    except Exception as e:
        print(f"Error creating topics: {e}")
    finally:
        admin_client.close()
