"""
Person-based email processing and validation.
Processes Person objects instead of raw EmailEntry objects.
"""

from typing import List, Dict, Tuple
import pandas as pd
from models.Person import Person
from models.EmailEntry import EmailEntry
from .email_hygeine_engine import validate_email_deterministic, load_reference_sets


class PersonEmailProcessor:
    """
    Processes Person objects for email validation and deduplication.
    """
    
    def __init__(self, options: Dict):
        self.options = options
        self.reference_sets = load_reference_sets()
        
    def process_people_emails(self, people: List[Person]) -> Tuple[List[Person], List[Person], Dict[str, int]]:
        """
        Process all people for email validation and deduplication.
        
        Returns:
            Tuple of (accepted_people, rejected_people, summary_stats)
        """
        accepted_people = []
        rejected_people = []
        summary = {
            'total_processed': len(people),
            'accepted': 0,
            'fixed': 0,
            'removed': 0,
            'duplicates': 0
        }
        
        # First pass: validate all emails
        validated_people = []
        for person in people:
            if person.has_email():
                validated_person = self._validate_person_email(person)
                validated_people.append(validated_person)
            else:
                # Person has no email - mark as rejected
                person.email_validated = True
                person.email_action = "removed"
                person.email_rejection_reason = "No email provided"
                person.email_confidence = 0.0
                rejected_people.append(person)
                summary['removed'] += 1
        
        # Second pass: deduplication
        accepted_people = self._deduplicate_people(validated_people)
        
        # Categorize results
        for person in validated_people:
            if person.is_email_accepted():
                if person.email_action == "accepted":
                    summary['accepted'] += 1
                elif person.email_action == "fixed":
                    summary['fixed'] += 1
            else:
                if person.email_action == "removed":
                    summary['removed'] += 1
                elif person.email_action == "duplicate":
                    summary['duplicates'] += 1
                rejected_people.append(person)
        
        return accepted_people, rejected_people, summary
    
    def _validate_person_email(self, person: Person) -> Person:
        """
        Validate a single person's email and update the person object.
        """
        if not person.has_email():
            person.email_validated = True
            person.email_action = "removed"
            person.email_rejection_reason = "No email provided"
            person.email_confidence = 0.0
            return person
        
        # Get the email entry
        email_entry = person.email
        
        # Validate the email
        validation_result = validate_email_deterministic(
            email_entry.raw,
            exclude_role_accounts=self.options.get('exclude_role_accounts', False),
            disposable_set=self.reference_sets.get('disposable_domains'),
            role_locals=self.reference_sets.get('role_locals'),
            top_domains=self.reference_sets.get('top_domains'),
            tld_whitelist=self.reference_sets.get('tld_whitelist')
        )
        
        # Update the person's email validation fields
        person.email_validated = True
        
        # Map validation actions to our action types
        if validation_result.action == "accept":
            person.email_action = "accepted"
        elif validation_result.action == "fix_auto":
            person.email_action = "fixed"
        elif validation_result.action == "review":
            person.email_action = "accepted"  # Treat review as accepted for now
        else:  # suppress
            person.email_action = "removed"
        
        person.email_rejection_reason = '; '.join(validation_result.risk_reasons) if validation_result.risk_reasons else ''
        person.email_confidence = validation_result.confidence
        
        # Update the email entry if it was fixed
        if validation_result.action == 'fix_auto':
            email_entry.cleaned = validation_result.suggested_fix or email_entry.raw
            email_entry.action = 'fixed'
            email_entry.reason = '; '.join(validation_result.risk_reasons) if validation_result.risk_reasons else ''
            email_entry.confidence = validation_result.confidence
            email_entry.changed = True
            email_entry.canonical_key = validation_result.canonical_key
        
        return person
    
    def _deduplicate_people(self, people: List[Person]) -> List[Person]:
        """
        Remove duplicate people based on email addresses.
        Keeps the first occurrence of each unique email.
        """
        seen_emails = set()
        unique_people = []
        
        for person in people:
            if person.has_email():
                email_key = person.email.canonical_key or person.email.raw.lower().strip()
                
                if email_key in seen_emails:
                    # Mark as duplicate
                    person.email_action = "duplicate"
                    person.email_rejection_reason = f"Duplicate email: {person.email.raw}"
                else:
                    seen_emails.add(email_key)
                    unique_people.append(person)
            else:
                # Person without email - keep them
                unique_people.append(person)
        
        return unique_people
