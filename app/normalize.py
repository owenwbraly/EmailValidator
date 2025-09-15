"""
Email normalization utilities
Applies deterministic cleanup and standardization
"""

import re
import unicodedata
from typing import Dict, Any, Optional


class EmailNormalizer:
    def __init__(self):
        # Compile regex patterns for efficiency
        self.whitespace_pattern = re.compile(r'\s+')
        self.zero_width_pattern = re.compile(r'[\u200b\u200c\u200d\u2060]')
        self.smart_quotes_pattern = re.compile(r'["""''`]')
        self.consecutive_dots_pattern = re.compile(r'\.{2,}')
    
    def normalize_email(self, email: str, features: Dict[str, Any]) -> str:
        """
        Apply comprehensive normalization to email address
        Returns normalized email string
        """
        if not email or not isinstance(email, str):
            return email
        
        normalized = email
        
        # Step 1: Basic cleanup
        normalized = self._basic_cleanup(normalized)
        
        # Step 2: Character replacements
        normalized = self._character_replacements(normalized)
        
        # Step 3: Domain-specific normalization
        normalized = self._domain_normalization(normalized, features)
        
        # Step 4: Apply corrections if safe
        normalized = self._apply_safe_corrections(normalized, features)
        
        return normalized
    
    def _basic_cleanup(self, email: str) -> str:
        """Apply basic cleanup operations"""
        # Remove leading/trailing whitespace
        email = email.strip()
        
        # Remove zero-width characters
        email = self.zero_width_pattern.sub('', email)
        
        # Remove surrounding angle brackets
        if email.startswith('<') and email.endswith('>'):
            email = email[1:-1]
        
        # Remove internal whitespace
        email = self.whitespace_pattern.sub('', email)
        
        return email
    
    def _character_replacements(self, email: str) -> str:
        """Apply character replacements"""
        # Replace smart quotes with regular quotes
        email = self.smart_quotes_pattern.sub('"', email)
        
        # Replace fullwidth @ with regular @
        email = email.replace('＠', '@')
        
        return email
    
    def _domain_normalization(self, email: str, features: Dict[str, Any]) -> str:
        """Apply domain-specific normalization"""
        if '@' not in email:
            return email
        
        try:
            local, domain = email.rsplit('@', 1)
            
            # Lowercase domain only (preserve local case)
            domain = domain.lower()
            
            # Strip trailing dot from domain
            domain = domain.rstrip('.')
            
            # Collapse consecutive dots in domain
            domain = self.consecutive_dots_pattern.sub('.', domain)
            
            # Handle IDN/punycode if needed
            if 'punycode_domain' in features:
                domain = features['punycode_domain']
            
            return f"{local}@{domain}"
            
        except Exception:
            return email
    
    def _apply_safe_corrections(self, email: str, features: Dict[str, Any]) -> str:
        """Apply safe corrections based on feature suggestions"""
        if '@' not in email:
            return email
        
        # Apply high-confidence corrections
        for correction in features.get('suggested_corrections', []):
            if correction['confidence'] >= 0.90:
                if correction['type'] in ['tld_typo', 'domain_typo']:
                    local, domain = email.rsplit('@', 1)
                    if domain.lower() == correction['original']:
                        return f"{local}@{correction['suggested']}"
        
        # Safe local part corrections
        local, domain = email.rsplit('@', 1)
        
        # Collapse double dots in local if unambiguous
        if '..' in local and not local.startswith('.') and not local.endswith('.'):
            # Only collapse if it results in valid local part
            collapsed_local = re.sub(r'\.{2,}', '.', local)
            if self._is_valid_local_part(collapsed_local):
                return f"{collapsed_local}@{domain}"
        
        return email
    
    def _is_valid_local_part(self, local: str) -> bool:
        """Check if local part is valid after normalization"""
        if not local or len(local) > 64:
            return False
        
        if local.startswith('.') or local.endswith('.'):
            return False
        
        if '..' in local:
            return False
        
        # Check for valid characters (simplified)
        if re.search(r'[^a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]', local):
            return False
        
        return True
    
    def get_canonical_form(self, email: str, provider_aware: bool = True) -> str:
        """
        Get canonical form for de-duplication
        Applies provider-specific rules for Gmail, etc.
        """
        if '@' not in email:
            return email.lower()
        
        local, domain = email.rsplit('@', 1)
        domain = domain.lower()
        
        if provider_aware:
            # Gmail-specific rules
            if domain in ['gmail.com', 'googlemail.com']:
                # Remove dots and plus-tags
                local = local.replace('.', '')
                if '+' in local:
                    local = local.split('+')[0]
            
            # Outlook-specific rules  
            elif domain in ['outlook.com', 'hotmail.com', 'live.com']:
                # Remove plus-tags but keep dots
                if '+' in local:
                    local = local.split('+')[0]
        
        # General normalization
        local = local.lower()
        
        return f"{local}@{domain}"
