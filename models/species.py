"""Species data model."""

from datetime import datetime
from typing import Dict, Any, Optional


class Species:
    """Represents a bird species."""

    def __init__(self,
                 taxonomy_id: str,
                 species_name: str,
                 common_name: Optional[str] = None,
                 scientific_name: Optional[str] = None,
                 family: Optional[str] = None,
                 order: Optional[str] = None,
                 additional_data: Optional[Dict[str, Any]] = None):
        """
        Initialize a Species instance.

        Args:
            taxonomy_id: Unique taxonomy identifier
            species_name: Name of the species
            common_name: Common name of the species
            scientific_name: Scientific name of the species
            family: Taxonomic family
            order: Taxonomic order
            additional_data: Any additional scraped data
        """
        self.taxonomy_id = taxonomy_id
        self.species_name = species_name
        self.common_name = common_name
        self.scientific_name = scientific_name
        self.family = family
        self.order = order
        self.additional_data = additional_data or {}
        self.created_at = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return {
            'taxonomy_id': self.taxonomy_id,
            'species_name': self.species_name,
            'common_name': self.common_name,
            'scientific_name': self.scientific_name,
            'family': self.family,
            'order': self.order,
            'additional_data': self.additional_data,
            'created_at': self.created_at
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Species':
        """Create Species instance from dictionary."""
        return cls(
            taxonomy_id=data['taxonomy_id'],
            species_name=data['species_name'],
            common_name=data.get('common_name'),
            scientific_name=data.get('scientific_name'),
            family=data.get('family'),
            order=data.get('order'),
            additional_data=data.get('additional_data', {})
        )
