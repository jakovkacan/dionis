"""Script to process audio files, upload to MinIO, and classify birds."""

import sys
import os
import json
import requests
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import yaml
from config.database import get_database, get_collection
from config.storage import get_minio_client, ensure_buckets, upload_file, upload_bytes
from models.audio_file import AudioFile
from models.classification import Classification


def load_config():
    """Load configuration."""
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)


def get_audio_files(directory: str) -> List[Path]:
    """
    Get all audio files from directory.

    Args:
        directory: Path to directory

    Returns:
        List of audio file paths
    """
    audio_extensions = {'.mp3', '.wav', '.flac', '.ogg', '.m4a', '.aac'}
    audio_files = []

    directory_path = Path(directory)
    if not directory_path.exists():
        print(f"✗ Directory not found: {directory}")
        return audio_files

    for file_path in directory_path.rglob('*'):
        if file_path.is_file() and file_path.suffix.lower() in audio_extensions:
            audio_files.append(file_path)

    return audio_files


def upload_audio_to_minio(minio_client, file_path: Path,
                          bucket_name: str) -> str:
    """
    Upload audio file to MinIO.

    Args:
        minio_client: MinIO client
        file_path: Path to audio file
        bucket_name: Target bucket name

    Returns:
        Object path in MinIO
    """
    # Generate unique object name using hash
    with open(file_path, 'rb') as f:
        file_hash = hashlib.md5(f.read()).hexdigest()

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    object_name = f"audio/{timestamp}_{file_hash}_{file_path.name}"

    # Determine content type
    content_type_map = {
        '.mp3': 'audio/mpeg',
        '.wav': 'audio/wav',
        '.flac': 'audio/flac',
        '.ogg': 'audio/ogg',
        '.m4a': 'audio/mp4',
        '.aac': 'audio/aac'
    }
    content_type = content_type_map.get(file_path.suffix.lower(), 'application/octet-stream')

    # Upload file
    minio_path = upload_file(
        client=minio_client,
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=str(file_path),
        content_type=content_type
    )

    return minio_path


def classify_audio(api_endpoint: str, file_path: Path,
                   timeout: int = 60) -> Optional[Dict[str, Any]]:
    """
    Call classification API for audio file.

    Args:
        api_endpoint: API endpoint URL
        file_path: Path to audio file
        timeout: Request timeout

    Returns:
        Classification response or None
    """
    try:
        # Prepare file for upload
        with open(file_path, 'rb') as f:
            files = {'audio': (file_path.name, f, 'audio/mpeg')}

            # Make API request
            response = requests.post(
                api_endpoint,
                files=files,
                timeout=timeout
            )
            response.raise_for_status()

            return response.json()

    except requests.RequestException as e:
        print(f"✗ API request failed: {e}")
        return None
    except Exception as e:
        print(f"✗ Error classifying audio: {e}")
        return None


def store_api_log(minio_client, bucket_name: str,
                  file_name: str, request_data: Dict[str, Any],
                  response_data: Dict[str, Any]) -> str:
    """
    Store API request/response log in MinIO.

    Args:
        minio_client: MinIO client
        bucket_name: Target bucket name
        file_name: Original audio file name
        request_data: Request information
        response_data: Response data

    Returns:
        Log path in MinIO
    """
    log_data = {
        'timestamp': datetime.utcnow().isoformat(),
        'file_name': file_name,
        'request': request_data,
        'response': response_data
    }

    log_json = json.dumps(log_data, indent=2).encode('utf-8')

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    object_name = f"logs/{timestamp}_{file_name}.json"

    log_path = upload_bytes(
        client=minio_client,
        bucket_name=bucket_name,
        object_name=object_name,
        data=log_json,
        content_type='application/json'
    )

    return log_path


def extract_location_from_path(file_path: Path,
                               default_lat: float,
                               default_lon: float) -> tuple:
    """
    Extract location from file path or use default.

    Args:
        file_path: Path to audio file
        default_lat: Default latitude
        default_lon: Default longitude

    Returns:
        Tuple of (latitude, longitude)
    """
    # Check if parent folder name contains coordinates
    # Format: location_lat45.8150_lon15.9819
    parent_name = file_path.parent.name

    if 'lat' in parent_name.lower() and 'lon' in parent_name.lower():
        try:
            parts = parent_name.split('_')
            lat = None
            lon = None

            for part in parts:
                if part.lower().startswith('lat'):
                    lat = float(part[3:])
                elif part.lower().startswith('lon'):
                    lon = float(part[3:])

            if lat is not None and lon is not None:
                return lat, lon
        except ValueError:
            pass

    return default_lat, default_lon


def process_audio_files():
    """Main function to process audio files."""
    print("=" * 60)
    print("STEP 3: Processing Audio Files")
    print("=" * 60)

    config = load_config()

    # Connect to services
    db = get_database()
    audio_collection = get_collection('audio_files', db)
    classification_collection = get_collection('classifications', db)
    species_collection = get_collection('species', db)

    minio_client = get_minio_client()
    ensure_buckets(minio_client)

    # Get configuration
    audio_config = config['audio']
    api_config = config['classifier_api']
    minio_config = config['minio']

    source_dir = audio_config['source_directory']
    default_location = audio_config['default_location']
    audio_bucket = minio_config['buckets']['audio']
    logs_bucket = minio_config['buckets']['logs']

    # Get audio files
    print(f"Scanning directory: {source_dir}")
    audio_files = get_audio_files(source_dir)

    if not audio_files:
        print(f"✗ No audio files found in {source_dir}")
        return

    print(f"✓ Found {len(audio_files)} audio files")

    uploaded_count = 0
    classified_count = 0

    for idx, file_path in enumerate(audio_files, 1):
        print(f"\n[{idx}/{len(audio_files)}] Processing: {file_path.name}")

        try:
            # Extract location
            latitude, longitude = extract_location_from_path(
                file_path,
                default_location['latitude'],
                default_location['longitude']
            )

            # Upload to MinIO
            print("  Uploading to MinIO...")
            minio_path = upload_audio_to_minio(
                minio_client,
                file_path,
                audio_bucket
            )

            # Create AudioFile record
            audio_file = AudioFile(
                filename=file_path.name,
                minio_path=minio_path,
                latitude=latitude,
                longitude=longitude,
                file_size=file_path.stat().st_size,
                content_type='audio/mpeg'
            )

            # Store in MongoDB
            result = audio_collection.insert_one(audio_file.to_dict())
            audio_file_id = str(result.inserted_id)
            uploaded_count += 1

            print(f"  ✓ Uploaded to: {minio_path}")
            print(f"  ✓ Location: ({latitude:.4f}, {longitude:.4f})")

            # Classify audio
            print("  Calling classification API...")
            api_response = classify_audio(
                api_config['endpoint'],
                file_path,
                api_config['timeout']
            )

            if not api_response:
                print("  ✗ Classification failed")
                continue

            # Store API log
            log_path = store_api_log(
                minio_client,
                logs_bucket,
                file_path.name,
                {'file_path': str(file_path)},
                api_response
            )

            print(f"  ✓ API log stored: {log_path}")

            # Parse classification results
            detected_birds = api_response.get('detections', [])

            if not detected_birds:
                print("  ℹ No birds detected")
                continue

            # Store classifications
            for detection in detected_birds:
                taxonomy_id = detection.get('taxonomy_id', detection.get('species_id', ''))
                confidence = float(detection.get('confidence', 0.0))
                species_name = detection.get('species_name', 'Unknown')

                # Look up species in database
                species = species_collection.find_one({'taxonomy_id': taxonomy_id})
                if species:
                    species_name = species.get('species_name', species_name)

                # Create classification record
                classification = Classification(
                    audio_file_id=audio_file_id,
                    taxonomy_id=taxonomy_id,
                    confidence=confidence,
                    species_name=species_name,
                    detected_birds=detected_birds,
                    api_response=api_response,
                    log_path=log_path
                )

                classification_collection.insert_one(classification.to_dict())
                classified_count += 1

                print(f"  ✓ Classified: {species_name} (confidence: {confidence:.2%})")

        except Exception as e:
            print(f"  ✗ Error processing file: {e}")
            continue

    print("\n" + "=" * 60)
    print(f"✓ Uploaded {uploaded_count} audio files")
    print(f"✓ Created {classified_count} classification records")

    # Create checkpoint file
    Path('checkpoints').mkdir(exist_ok=True)
    Path('checkpoints/audio_processed.flag').touch()
    print("✓ Checkpoint created: audio_processed.flag")


if __name__ == '__main__':
    process_audio_files()
