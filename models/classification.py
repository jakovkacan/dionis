from datetime import datetime
from typing import Dict, Any, Optional, List
from bson import ObjectId


class Classification:
    """Represents a bird classification result."""

    def __init__(self,
                 audio_file_id: str,
                 key: int,
                 confidence: float,
                 scientific_name: str,
                 detected_birds: Optional[List[Dict[str, Any]]] = None,
                 api_response: Optional[Dict[str, Any]] = None,
                 log_path: Optional[str] = None):
        """
        Initialize a Classification instance.

        Args:
            audio_file_id: Reference to audio file
            key: GBIF species key (unique identifier)
            confidence: Classification confidence score
            scientific_name: Scientific name of detected species
            detected_birds: List of all detected birds
            api_response: Raw API response
            log_path: Path to API log in MinIO
        """
        self.audio_file_id = audio_file_id
        self.key = key
        self.confidence = confidence
        self.scientific_name = scientific_name
        self.detected_birds = detected_birds or []
        self.api_response = api_response or {}
        self.log_path = log_path
        self.created_at = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return {
            'audio_file_id': ObjectId(self.audio_file_id) if isinstance(self.audio_file_id,
                                                                        str) else self.audio_file_id,
            'key': self.key,
            'confidence': self.confidence,
            'scientific_name': self.scientific_name,
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
            key=data['key'],
            confidence=data['confidence'],
            scientific_name=data['scientific_name'],
            detected_birds=data.get('detected_birds', []),
            api_response=data.get('api_response', {}),
            log_path=data.get('log_path')
        )
