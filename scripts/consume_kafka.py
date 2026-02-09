"""Script to consume bird observations from Kafka and store in MongoDB."""

import sys
from pathlib import Path
from typing import Dict, Any

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import get_database, get_collection
from config.kafka_config import get_kafka_consumer
from models.observation import Observation


def parse_observation_message(message: Dict[str, Any]) -> Observation:
    """
    Parse Kafka message into Observation object.

    Args:
        message: Kafka message data

    Returns:
        Observation object
    """
    # Extract required fields
    taxonomy_id = message.get('taxonomy_id') or message.get('species_id', '')
    latitude = float(message.get('latitude', 0.0))
    longitude = float(message.get('longitude', 0.0))

    # Extract biological data (all other fields)
    exclude_keys = {'taxonomy_id', 'species_id', 'latitude', 'longitude', 'timestamp'}
    biological_data = {
        key: value for key, value in message.items()
        if key not in exclude_keys
    }

    # Create observation
    observation = Observation(
        taxonomy_id=taxonomy_id,
        latitude=latitude,
        longitude=longitude,
        timestamp=message.get('timestamp'),
        biological_data=biological_data,
        source='kafka'
    )

    return observation


def consume_and_store_observations():
    """Main function to consume Kafka messages and store observations."""
    print("=" * 60)
    print("STEP 2: Consuming Bird Observations from Kafka")
    print("=" * 60)

    # Connect to MongoDB
    db = get_database()
    observations_collection = get_collection('observations', db)

    # Connect to Kafka
    consumer = get_kafka_consumer()

    print("Consuming messages from Kafka...")
    print("(Will timeout after 10 seconds of no new messages)")

    message_count = 0
    stored_count = 0

    try:
        # Consume all available messages
        for message in consumer:
            message_count += 1

            try:
                # Parse message
                observation = parse_observation_message(message.value)

                # Store in MongoDB
                result = observations_collection.insert_one(observation.to_dict())

                if result.inserted_id:
                    stored_count += 1
                    print(f"Stored observation {stored_count}: "
                          f"Species {observation.taxonomy_id} at "
                          f"({observation.latitude:.4f}, {observation.longitude:.4f})")

            except Exception as e:
                print(f"Error processing message {message_count}: {e}")
                continue

    except Exception as e:
        print(f"Error consuming from Kafka: {e}")

    finally:
        consumer.close()
        print(f"\nConsumed {message_count} messages")
        print(f"Stored {stored_count} observations in MongoDB")

        # Create checkpoint file
        Path('checkpoints').mkdir(exist_ok=True)
        Path('checkpoints/kafka_consumed.flag').touch()
        print("Checkpoint created: kafka_consumed.flag")


if __name__ == '__main__':
    consume_and_store_observations()
