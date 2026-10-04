"""Shared paths and settings, overridable through .env / environment variables."""
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

RAW_DIR = Path(os.getenv("RAW_DIR", ROOT / "raw_data"))
CHECKPOINT_DIR = Path(os.getenv("CHECKPOINT_DIR", ROOT / "checkpoints"))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "disaster_db")
