"""Data cleaning and transformation utilities."""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional
from rapidfuzz import fuzz, process


class DataCleaner:
    """Data cleaning and transformation utilities."""

    @staticmethod
    def clean_observations(observations: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Clean and transform observation data.

        Args:
            observations: List of observation dictionaries

        Returns:
            Cleaned pandas DataFrame
        """
        df = pd.DataFrame(observations)

        if df.empty:
            return df

        # Handle missing values
        df = df.fillna({
            'latitude': 0.0,
            'longitude': 0.0,
            'confidence': 0.0
        })

        # Remove invalid coordinates
        df = df[
            (df['latitude'].between(-90, 90)) &
            (df['longitude'].between(-180, 180))
            ]

        # Remove duplicates
        df = df.drop_duplicates(subset=['key', 'latitude', 'longitude', 'timestamp'])

        # Convert timestamps
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

        return df

    @staticmethod
    def clean_classifications(classifications: List[Dict[str, Any]],
                              min_confidence: float = 0.5) -> pd.DataFrame:
        """
        Clean and filter classification data.

        Args:
            classifications: List of classification dictionaries
            min_confidence: Minimum confidence threshold

        Returns:
            Cleaned pandas DataFrame
        """
        df = pd.DataFrame(classifications)

        if df.empty:
            return df

        # Filter by confidence
        df = df[df['confidence'] >= min_confidence]

        # Remove duplicates (keep highest confidence)
        df = df.sort_values('confidence', ascending=False)
        df = df.drop_duplicates(subset=['audio_file_id', 'key'], keep='first')

        return df

    @staticmethod
    def filter_species_fuzzy(species_list: List[str],
                             filter_term: str,
                             threshold: int = 70) -> List[str]:
        """
        Fuzzy filter species names.

        Args:
            species_list: List of species names
            filter_term: Term to filter by
            threshold: Minimum similarity score (0-100)

        Returns:
            List of matching species names
        """
        if not filter_term:
            return species_list

        matches = process.extract(
            filter_term,
            species_list,
            scorer=fuzz.partial_ratio,
            score_cutoff=threshold
        )

        return [match[0] for match in matches]

    @staticmethod
    def aggregate_biological_data(observations: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate biological data from observations.

        Args:
            observations: DataFrame of observations

        Returns:
            Aggregated DataFrame
        """
        if observations.empty:
            return observations

        # Extract biological data if stored in nested structure
        bio_cols = []
        for col in observations.columns:
            if 'biological_data' in col or col.startswith('bio_'):
                bio_cols.append(col)

        # Create summary statistics for numerical biological data
        if bio_cols:
            agg_dict = {}
            for col in bio_cols:
                if pd.api.types.is_numeric_dtype(observations[col]):
                    agg_dict[col] = ['mean', 'min', 'max', 'std']

        return observations

    @staticmethod
    def handle_outliers(df: pd.DataFrame,
                        columns: List[str],
                        method: str = 'iqr') -> pd.DataFrame:
        """
        Handle outliers in numerical data.

        Args:
            df: DataFrame
            columns: Columns to check for outliers
            method: Method to use ('iqr' or 'zscore')

        Returns:
            DataFrame with outliers handled
        """
        df_clean = df.copy()

        for col in columns:
            if col not in df_clean.columns or not pd.api.types.is_numeric_dtype(df_clean[col]):
                continue

            if method == 'iqr':
                Q1 = df_clean[col].quantile(0.25)
                Q3 = df_clean[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                df_clean[col] = df_clean[col].clip(lower_bound, upper_bound)

            elif method == 'zscore':
                mean = df_clean[col].mean()
                std = df_clean[col].std()
                df_clean[col] = df_clean[col].clip(mean - 3 * std, mean + 3 * std)

        return df_clean
