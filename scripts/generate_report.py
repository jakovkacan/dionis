import sys
from pathlib import Path
from typing import Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
import pandas as pd
from config.database import get_database, get_collection
from utils.data_cleaner import DataCleaner


def load_config():
    """Load configuration."""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


def fetch_data_from_mongodb(db, species_filter: Optional[str] = None,
                            min_confidence: float = 0.5) -> pd.DataFrame:
    """
    Fetch and join data from MongoDB collections.

    Args:
        db: Database instance
        species_filter: Optional species name filter
        min_confidence: Minimum confidence threshold

    Returns:
        Combined DataFrame
    """
    # Get collections
    species_coll = get_collection('species', db)
    classifications_coll = get_collection('classifications', db)
    observations_coll = get_collection('observations', db)

    # Fetch all species
    species_docs = list(species_coll.find())
    species_df = pd.DataFrame(species_docs)

    if species_df.empty:
        print("No species data found")
        return pd.DataFrame()

    # Normalize column names - MongoDB stores with aliases
    # scientificName -> scientific_name, canonicalName -> canonical_name
    column_mapping = {
        'scientificName': 'scientific_name',
        'canonicalName': 'canonical_name'
    }
    species_df.rename(columns=column_mapping, inplace=True)

    # Apply fuzzy filter if provided
    if species_filter:
        print(f"Applying fuzzy filter: '{species_filter}'")
        # Check which name column exists
        name_column = 'scientific_name' if 'scientific_name' in species_df.columns else 'scientificName'
        species_names = species_df[name_column].tolist()
        filtered_names = DataCleaner.filter_species_fuzzy(
            species_names,
            species_filter,
            threshold=70
        )
        species_df = species_df[species_df[name_column].isin(filtered_names)]
        print(f"Filtered to {len(species_df)} species")

    # Fetch classifications with minimum confidence
    classification_docs = list(classifications_coll.find({
        'confidence': {'$gte': min_confidence}
    }))
    classifications_df = pd.DataFrame(classification_docs)

    if classifications_df.empty:
        print("No classifications found")
        return pd.DataFrame()

    # Clean classifications
    classifications_df = DataCleaner.clean_classifications(
        classification_docs,
        min_confidence=min_confidence
    )

    # Fetch observations
    observation_docs = list(observations_coll.find())
    observations_df = pd.DataFrame(observation_docs) if observation_docs else pd.DataFrame()

    # Merge data - select only columns that exist
    merge_columns = ['key']
    optional_columns = ['scientific_name', 'canonical_name', 'family', 'order']
    for col in optional_columns:
        if col in species_df.columns:
            merge_columns.append(col)

    result_df = classifications_df.merge(
        species_df[merge_columns],
        on='key',
        how='left',
        suffixes=('_class', '_species')
    )

    # Use scientific name from species collection if available
    if 'scientific_name_species' in result_df.columns and 'scientific_name_class' in result_df.columns:
        result_df['scientific_name'] = result_df['scientific_name_species'].fillna(
            result_df['scientific_name_class']
        )
        result_df = result_df.drop(columns=['scientific_name_class', 'scientific_name_species'])
    elif 'scientific_name_species' in result_df.columns:
        result_df = result_df.rename(columns={'scientific_name_species': 'scientific_name'})
    elif 'scientific_name_class' in result_df.columns:
        result_df = result_df.rename(columns={'scientific_name_class': 'scientific_name'})

    # Add observation data if available
    if not observations_df.empty:
        # Group observations by key
        obs_grouped = observations_df.groupby('key').agg({
            '_id': 'count',
            'biological_data': lambda x: x.tolist()
        }).reset_index()
        obs_grouped.columns = ['key', 'observation_count', 'biological_data_list']

        result_df = result_df.merge(
            obs_grouped,
            on='key',
            how='left'
        )

    return result_df


def aggregate_statistics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate statistics by species.

    Args:
        df: Combined DataFrame

    Returns:
        Aggregated statistics DataFrame
    """
    if df.empty:
        return df

    # Group by species
    agg_dict = {
        'confidence': ['mean', 'max', 'min', 'count'],
        'audio_file_id': 'nunique'
    }

    # Add observation count if available
    if 'observation_count' in df.columns:
        agg_dict['observation_count'] = 'sum'

    stats_df = df.groupby(['key', 'scientific_name']).agg(agg_dict).reset_index()

    # Flatten column names
    stats_df.columns = [
        'key',
        'scientific_name',
        'avg_confidence',
        'max_confidence',
        'min_confidence',
        'classification_count',
        'unique_audio_files',
        'total_observations'
    ] if 'observation_count' in df.columns else [
        'key',
        'scientific_name',
        'avg_confidence',
        'max_confidence',
        'min_confidence',
        'classification_count',
        'unique_audio_files'
    ]

    # Add biological data summary if available
    if 'biological_data_list' in df.columns:
        bio_data_summary = extract_biological_summary(df)
        stats_df = stats_df.merge(
            bio_data_summary,
            on='key',
            how='left'
        )

    # Sort by classification count
    stats_df = stats_df.sort_values('classification_count', ascending=False)

    return stats_df


def extract_biological_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract summary of biological data.

    Args:
        df: DataFrame with biological_data_list

    Returns:
        Summary DataFrame
    """
    bio_summaries = []

    for key, group in df.groupby('key'):
        bio_data_list = []

        for bio_list in group['biological_data_list'].dropna():
            if isinstance(bio_list, list):
                bio_data_list.extend(bio_list)

        if not bio_data_list:
            continue

        # Aggregate biological properties
        all_keys = set()
        for bio_data in bio_data_list:
            if isinstance(bio_data, dict):
                all_keys.update(bio_data.keys())

        summary = {'key': key}

        for key_name in all_keys:
            values = [
                bio_data.get(key_name) for bio_data in bio_data_list
                if isinstance(bio_data, dict) and key_name in bio_data
            ]

            if values:
                # Try to compute statistics for numerical values
                try:
                    numeric_values = [float(v) for v in values if v is not None]
                    if numeric_values:
                        summary[f'avg_{key_name}'] = sum(numeric_values) / len(numeric_values)
                except (ValueError, TypeError):
                    # For non-numeric, count occurrences
                    most_common = max(set(values), key=values.count)
                    summary[f'most_common_{key_name}'] = most_common

        bio_summaries.append(summary)

    return pd.DataFrame(bio_summaries) if bio_summaries else pd.DataFrame()


def generate_report():
    """Main function to generate statistics report."""
    print("=" * 60)
    print("STEP 4: Generating Statistics Report")
    print("=" * 60)

    config = load_config()

    # Get configuration
    report_config = config['report']
    output_file = report_config['output_file']
    min_confidence = report_config.get('min_confidence', 0.5)
    species_filter = report_config.get('species_filter')

    # Connect to MongoDB
    db = get_database()

    # Fetch data
    print("Fetching data from MongoDB...")
    df = fetch_data_from_mongodb(db, species_filter, min_confidence)

    if df.empty:
        print("No data to generate report")
        return

    print(f"Fetched {len(df)} classification records")

    # Clean data
    print("Cleaning data...")
    df = DataCleaner.clean_classifications(df.to_dict('records'), min_confidence)

    # Aggregate statistics
    print("Aggregating statistics...")
    stats_df = aggregate_statistics(df)

    if stats_df.empty:
        print("No statistics to generate")
        return

    print(f"Generated statistics for {len(stats_df)} species")

    # Ensure output directory exists
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Save to CSV
    stats_df.to_csv(output_file, index=False)
    print(f"Report saved to: {output_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("REPORT SUMMARY")
    print("=" * 60)
    print(f"Total species: {len(stats_df)}")
    print(f"Total classifications: {stats_df['classification_count'].sum():.0f}")
    print(f"Total unique audio files: {stats_df['unique_audio_files'].sum():.0f}")

    if 'total_observations' in stats_df.columns:
        print(f"Total observations: {stats_df['total_observations'].sum():.0f}")

    print("\nTop 10 species by classification count:")
    print(stats_df[['scientific_name', 'classification_count', 'avg_confidence']].head(10).to_string(index=False))

    print("\nPipeline completed successfully!")


if __name__ == '__main__':
    # Check for command line arguments
    if len(sys.argv) > 1:
        species_filter = sys.argv[1]
        print(f"Using species filter from command line: {species_filter}")

        # Update config
        config = load_config()
        config['report']['species_filter'] = species_filter
        with open('config.yaml', 'w') as f:
            yaml.dump(config, f)

    generate_report()
