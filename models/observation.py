from datetime import datetime
from typing import Dict, Any, Optional


class Observation:
    """Represents a bird observation from Kafka."""

    def __init__(self,
                 key: int,
                 latitude: float,
                 longitude: float,
                 timestamp: Optional[datetime] = None,
                 biological_data: Optional[Dict[str, Any]] = None,
                 source: str = 'kafka'):
        """
        Initialize an Observation instance.

        Args:
            key: GBIF species key (unique identifier)
            latitude: Observation latitude
            longitude: Observation longitude
            timestamp: Time of observation
            biological_data: Variable biological properties
            source: Source of the observation
        """
        self.key = key
        self.latitude = latitude
        self.longitude = longitude
        self.timestamp = timestamp or datetime.utcnow()
        self.biological_data = biological_data or {}
        self.source = source
        self.created_at = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return {
            'key': self.key,
            'location': {
                'latitude': self.latitude,
                'longitude': self.longitude
            },
            'timestamp': self.timestamp,
            'biological_data': self.biological_data,
            'source': self.source,
            'created_at': self.created_at
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Observation':
        """Create Observation instance from dictionary."""
        location = data.get('location', {})
        return cls(
            key=data['key'],
            latitude=location.get('latitude', 0.0),
            longitude=location.get('longitude', 0.0),
            timestamp=data.get('timestamp'),
            biological_data=data.get('biological_data', {}),
            source=data.get('source', 'kafka')
        )
