import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.kafka_ornithology_producer import OrnithologyDataProducer


def produce_observations(count: int = 50, region: str = 'HR'):
    """
    Produce bird observations to Kafka.

    Args:
        count: Number of observations to produce
        region: eBird region code (default: HR for Croatia)
    """
    print("=" * 60)
    print("STEP 1: Producing Bird Observations to Kafka")
    print("=" * 60)

    # Create producer (uses config.yaml and .env)
    producer = OrnithologyDataProducer()

    try:
        # Fetch observations from eBird (or use mock data)
        print(f"\nFetching {count} observations for region {region}...")
        observations = producer.fetch_ebird_observations(
            region_code=region,
            max_results=count
        )

        # Publish to Kafka
        if observations:
            producer.publish_observations(observations, delay_ms=50)
            print(f"\nSuccessfully produced {len(observations)} observations to Kafka")
        else:
            print("\nNo observations fetched")

    finally:
        producer.close()

    # Create checkpoint file
    Path('checkpoints').mkdir(exist_ok=True)
    Path('checkpoints/kafka_produced.flag').touch()
    print("Checkpoint created: kafka_produced.flag")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Produce bird observations to Kafka'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=50,
        help='Number of observations to produce (default: 50)'
    )
    parser.add_argument(
        '--region',
        default='HR',
        help='eBird region code (default: HR for Croatia)'
    )

    args = parser.parse_args()

    produce_observations(count=args.count, region=args.region)
