"""
Email processing module
Contains all email-related validation, cleaning, and deduplication logic
"""

from .email_deduper import EmailDeduplicator
from .email_hygeine_engine import validate_email_deterministic, load_reference_sets, canonical_key

__all__ = [
    'EmailDeduplicator', 
    'validate_email_deterministic',
    'load_reference_sets',
    'canonical_key'
]
