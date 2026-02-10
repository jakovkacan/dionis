import json
import time
import random
import requests
import os
import yaml
from kafka import KafkaProducer
from datetime import datetime
from typing import Dict, List, Optional


def load_config():
    """Load configuration from config.yaml."""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


class OrnithologyDataProducer:
    """
    Fetches bird observations from eBird API and publishes to Kafka topic
    """

    def __init__(
            self,
            kafka_bootstrap_servers: Optional[str] = None,
            topic_name: Optional[str] = None,
            ebird_api_key: Optional[str] = None
    ):
        """
        Initialize the producer

        Args:
            kafka_bootstrap_servers: Kafka broker address (optional, loads from config if not provided)
            topic_name: Kafka topic to publish to (optional, loads from config if not provided)
            ebird_api_key: eBird API key (optional, loads from .env if not provided)
        """
        # Load configuration
        config = load_config()
        kafka_config = config['kafka']

        # Use provided values or fall back to config/env
        self.kafka_bootstrap_servers = kafka_bootstrap_servers or os.getenv(
            'KAFKA_BOOTSTRAP_SERVERS',
            kafka_config['bootstrap_servers']
        )
        self.topic_name = topic_name or kafka_config['topic']
        self.ebird_api_key = ebird_api_key or os.getenv('EBIRD_API_KEY')

        # Initialize Kafka producer with JSON serialization
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='gzip'
        )

        print(f"Kafka producer connected to {self.kafka_bootstrap_servers}")
        print(f"Publishing to topic: {self.topic_name}")

    def fetch_ebird_observations(
            self,
            region_code: str = 'HR',  # Croatia
            max_results: int = 50
    ) -> List[Dict]:
        """
        Fetch recent bird observations from eBird API

        Args:
            region_code: Region code (HR for Croatia, US-CA for California, etc.)
            max_results: Maximum number of observations to fetch

        Returns:
            List of observation dictionaries
        """
        if not self.ebird_api_key:
            print("No eBird API key provided, using mock data")
            return self._generate_mock_observations(max_results)

        url = f"https://api.ebird.org/v2/data/obs/{region_code}/recent"
        headers = {'X-eBirdApiToken': self.ebird_api_key}
        params = {
            'maxResults': max_results,
            'back': 14  # Last 14 days
        }

        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching from eBird: {e}")
            print("Falling back to mock data")
            return self._generate_mock_observations(max_results)

    def _generate_mock_observations(self, count: int = 50) -> List[Dict]:
        """
        Generate mock bird observations for testing
        """
        # Common bird species codes and names
        birds = [
            ('houspa', 'Passer domesticus', 'House Sparrow'),
            ('eurcoo', 'Columba palumbus', 'Common Wood Pigeon'),
            ('eurbla', 'Turdus merula', 'Eurasian Blackbird'),
            ('gretit', 'Parus major', 'Great Tit'),
            ('eurmag', 'Pica pica', 'Eurasian Magpie'),
            ('comsta', 'Sturnus vulgaris', 'Common Starling'),
            ('mallar', 'Anas platyrhynchos', 'Mallard'),
            ('carwre', 'Troglodytes troglodytes', 'Eurasian Wren'),
            ('eurjay', 'Garrulus glandarius', 'Eurasian Jay'),
            ('hoocar', 'Corvus cornix', 'Hooded Crow')
        ]

        # Zagreb area coordinates with some variance
        base_lat, base_lng = 45.8150, 15.9819

        observations = []
        for i in range(count):
            bird = random.choice(birds)

            obs = {
                'speciesCode': bird[0],
                'sciName': bird[1],
                'comName': bird[2],
                'lat': round(base_lat + random.uniform(-0.1, 0.1), 6),
                'lng': round(base_lng + random.uniform(-0.1, 0.1), 6),
                'obsDt': datetime.now().isoformat(),
                'howMany': random.randint(1, 15),
                'locName': f'Zagreb Location {random.randint(1, 20)}'
            }
            observations.append(obs)

        return observations

    def transform_to_observation_message(self, ebird_data: Dict) -> Dict:
        """
        Transform eBird data to the format expected by your pipeline.
        Format matches what consume_kafka.py expects.
        """
        # Extract species code as key (taxonomy_id)
        species_code = ebird_data.get('speciesCode', 'unknown')
        # Try to convert to integer if possible, otherwise use hash
        try:
            key = int(species_code) if species_code.isdigit() else abs(hash(species_code)) % (10 ** 8)
        except:
            key = 0

        # Base message with required fields
        message = {
            'key': key,
            'taxonomy_id': species_code,
            'latitude': ebird_data.get('lat'),
            'longitude': ebird_data.get('lng'),
            'timestamp': ebird_data.get('obsDt', datetime.now().isoformat()),
            'scientific_name': ebird_data.get('sciName', ''),
            'common_name': ebird_data.get('comName', ''),
            'source': 'ebird'
        }

        # Add optional observation data
        if 'howMany' in ebird_data:
            message['count'] = ebird_data['howMany']

        if 'locName' in ebird_data:
            message['location_name'] = ebird_data['locName']

        # Add random biological properties to simulate variance
        optional_properties = [
            ('migration_status', random.choice(['resident', 'migrant', 'winter_visitor', 'summer_visitor'])),
            ('flight_pattern', random.choice(['direct', 'undulating', 'hovering', 'soaring'])),
            ('habitat', random.choice(['urban', 'forest', 'wetland', 'grassland', 'coastal'])),
            ('body_size_cm', round(random.uniform(10, 60), 1)),
            ('body_temperature_c', round(random.uniform(38, 42), 1)),
        ]

        # Randomly include some biological properties (simulating variance)
        for prop, value in random.sample(optional_properties, k=random.randint(2, 4)):
            message[prop] = value

        return message

    def publish_observations(
            self,
            observations: List[Dict],
            delay_ms: int = 100
    ):
        """
        Publish observations to Kafka topic

        Args:
            observations: List of observation dictionaries
            delay_ms: Delay between messages in milliseconds
        """
        print(f"\nPublishing {len(observations)} observations to Kafka...")

        for i, obs in enumerate(observations, 1):
            try:
                # Transform to expected format
                message = self.transform_to_observation_message(obs)

                # Use key for partitioning
                key = str(message['key'])

                # Send to Kafka
                future = self.producer.send(
                    self.topic_name,
                    key=key,
                    value=message
                )

                # Wait for confirmation (optional, for demo purposes)
                metadata = future.get(timeout=10)

                print(f"  [{i}/{len(observations)}] {message['common_name']} "
                      f"(partition: {metadata.partition}, offset: {metadata.offset})")

                # Small delay to simulate real-time streaming
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)

            except Exception as e:
                print(f"  [{i}/{len(observations)}] Error: {e}")

        # Ensure all messages are sent
        self.producer.flush()
        print(f"\nSuccessfully published {len(observations)} observations")

    def run_continuous(
            self,
            region_code: str = 'HR',
            interval_seconds: int = 300,
            max_iterations: Optional[int] = None
    ):
        """
        Continuously fetch and publish observations

        Args:
            region_code: eBird region code
            interval_seconds: Seconds between fetches
            max_iterations: Maximum iterations (None for infinite)
        """
        iteration = 0
        print(f"\nStarting continuous publishing (interval: {interval_seconds}s)\n")

        try:
            while max_iterations is None or iteration < max_iterations:
                iteration += 1
                print(f"\n{'=' * 60}")
                print(f"Iteration {iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'=' * 60}")

                # Fetch observations
                observations = self.fetch_ebird_observations(
                    region_code=region_code,
                    max_results=random.randint(10, 30)  # Vary the count
                )

                # Publish to Kafka
                if observations:
                    self.publish_observations(observations)
                else:
                    print("No observations fetched")

                # Wait before next iteration
                if max_iterations is None or iteration < max_iterations:
                    print(f"\nWaiting {interval_seconds} seconds until next fetch...")
                    time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\n\nStopped by user")
        finally:
            self.close()

    def close(self):
        """Close the producer connection"""
        self.producer.close()
        print("\nProducer closed")


def main():
    """
    Main entry point
    """
    import argparse

    # Load config for defaults
    config = load_config()
    kafka_config = config['kafka']

    parser = argparse.ArgumentParser(
        description='Publish bird observations to Kafka from eBird API'
    )
    parser.add_argument(
        '--kafka-server',
        default=None,
        help=f'Kafka bootstrap server (default: from config.yaml: {kafka_config["bootstrap_servers"]})'
    )
    parser.add_argument(
        '--topic',
        default=None,
        help=f'Kafka topic name (default: from config.yaml: {kafka_config["topic"]})'
    )
    parser.add_argument(
        '--api-key',
        default=None,
        help='eBird API key (default: from .env, uses mock data if not provided)'
    )
    parser.add_argument(
        '--region',
        default='HR',
        help='eBird region code (default: HR for Croatia)'
    )
    parser.add_argument(
        '--mode',
        choices=['once', 'continuous'],
        default='once',
        help='Publishing mode: once or continuous (default: once)'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=300,
        help='Interval in seconds for continuous mode (default: 300)'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=50,
        help='Number of observations to fetch (default: 50)'
    )

    args = parser.parse_args()

    # Create producer (will use config/env if args not provided)
    producer = OrnithologyDataProducer(
        kafka_bootstrap_servers=args.kafka_server,
        topic_name=args.topic,
        ebird_api_key=args.api_key
    )

    if args.mode == 'once':
        # Single fetch and publish
        observations = producer.fetch_ebird_observations(
            region_code=args.region,
            max_results=args.count
        )
        producer.publish_observations(observations)
        producer.close()
    else:
        # Continuous mode
        producer.run_continuous(
            region_code=args.region,
            interval_seconds=args.interval
        )


if __name__ == '__main__':
    main()
