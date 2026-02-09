"""Script to scrape bird species data and store in MongoDB."""

import sys
import yaml
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.database import get_database, create_indexes
from utils.scraper import BirdSpeciesScraper
from models.species import Species, SpeciesRepository


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
    species_repo = SpeciesRepository(db)

    # Create indexes
    create_indexes(db)

    # Check if data already exists
    existing_count = species_repo.count()
    if existing_count > 0:
        print(f"Species data already exists ({existing_count} records)")
        print("Skipping scraping step...")
        return

    # Initialize scraper
    scraping_config = config['scraping']
    use_selenium = scraping_config.get('use_selenium', True)

    # Allow command line override
    if len(sys.argv) > 1 and sys.argv[1] == '--selenium':
        use_selenium = True
        print("Using Selenium for JavaScript-rendered content")

    scraper = BirdSpeciesScraper(
        base_url=scraping_config['url'],
        timeout=scraping_config['timeout'],
        retry_attempts=scraping_config['retry_attempts'],
        use_selenium=use_selenium
    )

    print(f"Starting scraping from: {scraping_config['url']}")

    try:
        # Scrape species data
        species_data_list = scraper.scrape_species_list()

        if not species_data_list:
            print("No species data scraped")
            return

        print(f"Scraped {len(species_data_list)} species")

        # Store in MongoDB
        inserted_count = 0
        updated_count = 0
        for species_data in species_data_list:
            try:
                # Create Species object from GBIF data
                species = Species(
                    key=species_data.get('key'),
                    scientificName=species_data.get('scientificName', ''),
                    canonicalName=species_data.get('canonicalName'),
                    rank=species_data.get('rank'),
                    kingdom=species_data.get('kingdom'),
                    phylum=species_data.get('phylum'),
                    **{'class': species_data.get('class')},  # 'class' is a Python keyword
                    order=species_data.get('order'),
                    family=species_data.get('family'),
                    genus=species_data.get('genus')
                )

                # Upsert species (insert or update)
                result = species_repo.upsert_species(species)
                if result:
                    updated_count += 1
                else:
                    inserted_count += 1

            except Exception as e:
                print(f"Error storing species {species_data.get('key')}: {e}")
                continue

        print(f"Inserted {inserted_count} new species into MongoDB")
        if updated_count > 0:
            print(f"Updated {updated_count} existing species")

        # Create checkpoint file
        Path('checkpoints').mkdir(exist_ok=True)
        Path('checkpoints/species_scraped.flag').touch()
        print("Checkpoint created: species_scraped.flag")

    except Exception as e:
        print(f"Error during scraping: {e}")
        raise


if __name__ == '__main__':
    scrape_and_store_species()
