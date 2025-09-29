"""
Models package for EmailValidator
Contains data models for different types of validation (emails, people, etc.)
"""

from .EmailEntry import EmailEntry, EmailArrayExtractor
from .Person import Person, PersonExtractor

__all__ = ['EmailEntry', 'EmailArrayExtractor', 'Person', 'PersonExtractor']
