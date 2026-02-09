from datetime import datetime
from typing import Dict, Any, Optional


class AudioFile:
    """Represents an audio file in storage."""

    def __init__(self,
                 filename: str,
                 minio_path: str,
                 latitude: float,
                 longitude: float,
                 file_size: Optional[int] = None,
                 content_type: str = 'audio/mpeg',
                 metadata: Optional[Dict[str, Any]] = None):
        """
        Initialize an AudioFile instance.

        Args:
            filename: Original filename
            minio_path: Path in MinIO storage
            latitude: Recording location latitude
            longitude: Recording location longitude
            file_size: Size in bytes
            content_type: MIME type
            metadata: Additional metadata
        """
        self.filename = filename
        self.minio_path = minio_path
        self.latitude = latitude
        self.longitude = longitude
        self.file_size = file_size
        self.content_type = content_type
        self.metadata = metadata or {}
        self.uploaded_at = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for MongoDB storage."""
        return {
            'filename': self.filename,
            'minio_path': self.minio_path,
            'location': {
                'latitude': self.latitude,
                'longitude': self.longitude
            },
            'file_size': self.file_size,
            'content_type': self.content_type,
            'metadata': self.metadata,
            'uploaded_at': self.uploaded_at
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AudioFile':
        """Create AudioFile instance from dictionary."""
        location = data.get('location', {})
        return cls(
            filename=data['filename'],
            minio_path=data['minio_path'],
            latitude=location.get('latitude', 0.0),
            longitude=location.get('longitude', 0.0),
            file_size=data.get('file_size'),
            content_type=data.get('content_type', 'audio/mpeg'),
            metadata=data.get('metadata', {})
        )
