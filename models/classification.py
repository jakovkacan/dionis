"""Classification data model."""

from datetime import datetime
from typing import Dict, Any, Optional, List
from bson import ObjectId


class Classification:
    """Represents a bird classification result."""

    def __init__(self,
                 audio_file_id: str,
                 taxonomy_id: str,
                 confidence: float,
                 species_name: str,
                 detected_birds: Optional[List[Dict[str, Any]]] = None,
                 api_response: Optional[Dict[str, Any]] = None,
                 log_path: Optional[str] = None):
        """
        Initialize a Classification instance.

        Args:
            audio_file_id: Reference to audio file
            taxonomy_id: Species taxonomy identifier
            confidence: Classification confidence score
            species_name: Name of detected species
            detected_birds: List of all detected birds
            api_response: Raw API response
            log_path: Path to API log in MinIO
        """
        self.audio_file_id = audio_file_id
        self.taxonomy_id = taxonomy_id
        self.confidence = confidence
        self.species_name = species_name
        self.detected_birds = detected_birds or []
        self.api_response = api_response or {}
        self.log_path = log_path
        self.created_at = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return {
            'audio_file_id': ObjectId(self.audio_file_id) if isinstance(self.audio_file_id,
                                                                        str) else self.audio_file_id,
            'taxonomy_id': self.taxonomy_id,
            'confidence': self.confidence,
            'species_name': self.species_name,
            'detected_birds': self.detected_birds,
            'api_response': self.api_response,
            'log_path': self.log_path,
            'created_at': self.created_at
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Classification':
        """Create Classification instance from dictionary."""
        return cls(
            audio_file_id=str(data['audio_file_id']),
            taxonomy_id=data['taxonomy_id'],
            confidence=data['confidence'],
            species_name=data['species_name'],
            detected_birds=data.get('detected_birds', []),
            api_response=data.get('api_response', {}),
            log_path=data.get('log_path')
        )
