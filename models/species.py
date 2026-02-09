from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator
from pydantic_core import core_schema
from bson import ObjectId


class PyObjectId(ObjectId):
    """Custom ObjectId type for Pydantic v2"""

    @classmethod
    def __get_pydantic_core_schema__(
            cls, source_type: Any, handler
    ) -> core_schema.CoreSchema:
        return core_schema.union_schema(
            [
                core_schema.is_instance_schema(ObjectId),
                core_schema.chain_schema(
                    [
                        core_schema.str_schema(),
                        core_schema.no_info_plain_validator_function(cls.validate),
                    ]
                ),
            ],
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda x: str(x)
            ),
        )

    @classmethod
    def validate(cls, v):
        if isinstance(v, ObjectId):
            return v
        if isinstance(v, str) and ObjectId.is_valid(v):
            return ObjectId(v)
        raise ValueError("Invalid ObjectId")


class Species(BaseModel):
    """
    Bird species model based on GBIF data structure
    """
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    key: int = Field(..., description="GBIF species key (unique identifier)")
    scientific_name: str = Field(..., alias="scientificName")
    canonical_name: Optional[str] = Field(None, alias="canonicalName")
    rank: Optional[str] = Field(None, description="Taxonomic rank (e.g., SPECIES, GENUS)")

    # Taxonomic hierarchy
    kingdom: Optional[str] = None
    phylum: Optional[str] = None
    class_name: Optional[str] = Field(None, alias="class")
    order: Optional[str] = None
    family: Optional[str] = None
    genus: Optional[str] = None

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        populate_by_name = True
        arbitrary_types_allowed = True
        json_encoders = {
            ObjectId: str,
            datetime: lambda dt: dt.isoformat()
        }
        json_schema_extra = {
            "example": {
                "key": 2480498,
                "scientificName": "Parus major",
                "canonicalName": "Parus major",
                "rank": "SPECIES",
                "kingdom": "Animalia",
                "phylum": "Chordata",
                "class": "Aves",
                "order": "Passeriformes",
                "family": "Paridae",
                "genus": "Parus"
            }
        }

    def to_mongo(self) -> dict:
        """Convert to MongoDB document format"""
        data = self.model_dump(by_alias=True, exclude_none=True)
        if data.get("_id") is None:
            data.pop("_id", None)
        return data

    @classmethod
    def from_mongo(cls, data: dict) -> "Species":
        """Create Species instance from MongoDB document"""
        if not data:
            return None
        return cls(**data)


class SpeciesRepository:
    """Repository for Species database operations"""

    def __init__(self, db):
        self.collection = db.species
        # Create unique index on GBIF key to avoid duplicates
        self.collection.create_index("key", unique=True)
        self.collection.create_index("scientificName")
        self.collection.create_index("family")
        self.collection.create_index("order")

    def insert_species(self, species: Species) -> Optional[str]:
        """Insert species if it doesn't exist"""
        try:
            result = self.collection.insert_one(species.to_mongo())
            return str(result.inserted_id)
        except Exception as e:
            # Skip if duplicate (key already exists)
            print(f"Species with key {species.key} already exists: {e}")
            return None

    def upsert_species(self, species: Species) -> bool:
        """Insert or update species"""
        species.updated_at = datetime.utcnow()
        result = self.collection.update_one(
            {"key": species.key},
            {"$set": species.to_mongo()},
            upsert=True
        )
        return result.modified_count > 0 or result.upserted_id is not None

    def find_by_key(self, key: int) -> Optional[Species]:
        """Find species by GBIF key"""
        doc = self.collection.find_one({"key": key})
        return Species.from_mongo(doc) if doc else None

    def find_by_name(self, name: str, fuzzy: bool = False) -> list[Species]:
        """Find species by scientific name (with optional fuzzy matching)"""
        if fuzzy:
            query = {"scientificName": {"$regex": name, "$options": "i"}}
        else:
            query = {"scientificName": name}

        docs = self.collection.find(query)
        return [Species.from_mongo(doc) for doc in docs]

    def get_all_species(self, limit: int = 0) -> list[Species]:
        """Get all species from collection"""
        docs = self.collection.find().limit(limit)
        return [Species.from_mongo(doc) for doc in docs]

    def count(self) -> int:
        """Count total species in collection"""
        return self.collection.count_documents({})
