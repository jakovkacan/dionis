"""Script to scrape bird species data and store in MongoDB."""

import sys
import yaml
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import get_database, get_collection, create_indexes
from utils.scraper import BirdSpeciesScraper
from models.species import Species


def load_config():
    """Load configuration."""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


def scrape_and_store_species():
    """Main function to scrape species data and store in MongoDB."""
    print("=" * 60)
    print("STEP 1: Scraping Bird Species Data")
    print("=" * 60)

    config = load_config()

    # Connect to MongoDB
    db = get_database()
    species_collection = get_collection('species', db)

    # Create indexes
    create_indexes(db)

    # Check if data already exists
    existing_count = species_collection.count_documents({})
    if existing_count > 0:
        print(f"✓ Species data already exists ({existing_count} records)")
        print("Skipping scraping step...")
        return

    # Initialize scraper
    scraping_config = config['scraping']
    scraper = BirdSpeciesScraper(
        base_url=scraping_config['url'],
        timeout=scraping_config['timeout'],
        retry_attempts=scraping_config['retry_attempts']
    )

    print(f"Starting scraping from: {scraping_config['url']}")

    try:
        # Scrape species data
        species_data_list = scraper.scrape_species_list()

        if not species_data_list:
            print("✗ No species data scraped")
            return

        print(f"✓ Scraped {len(species_data_list)} species")

        # Store in MongoDB
        inserted_count = 0
        for species_data in species_data_list:
            try:
                # Create Species object
                species = Species(
                    taxonomy_id=species_data.get('taxonomy_id', ''),
                    species_name=species_data.get('species_name', ''),
                    common_name=species_data.get('common_name'),
                    scientific_name=species_data.get('scientific_name'),
                    family=species_data.get('family'),
                    order=species_data.get('order'),
                    additional_data=species_data.get('additional_data', {})
                )

                # Insert into MongoDB (skip if taxonomy_id already exists)
                result = species_collection.update_one(
                    {'taxonomy_id': species.taxonomy_id},
                    {'$setOnInsert': species.to_dict()},
                    upsert=True
                )

                if result.upserted_id:
                    inserted_count += 1

            except Exception as e:
                print(f"✗ Error storing species {species_data.get('taxonomy_id')}: {e}")
                continue

        print(f"✓ Inserted {inserted_count} new species into MongoDB")

        # Create checkpoint file
        Path('checkpoints').mkdir(exist_ok=True)
        Path('checkpoints/species_scraped.flag').touch()
        print("✓ Checkpoint created: species_scraped.flag")

    except Exception as e:
        print(f"✗ Error during scraping: {e}")
        raise


if __name__ == '__main__':
    scrape_and_store_species()
