"""
Feature extraction for email validation
Extracts deterministic features and flags for LLM processing
"""

import re
import idna
import tld
from typing import Dict, Any, List, Set
import unicodedata
import json
import os


class FeatureExtractor:
    def __init__(self):
        self.load_config()
        
    def load_config(self):
        """Load configuration files for domain lists and typo maps"""
        # Load typo maps
        try:
            with open('config/typo_maps.json', 'r') as f:
                self.typo_maps = json.load(f)
        except FileNotFoundError:
            self.typo_maps = self._get_default_typo_maps()
        
        # Load domain lists
        self.disposable_domains = self._load_text_list('config/disposable_domains.txt')
        self.role_locals = self._load_text_list('config/role_locals.txt')
        self.top_domains = self._load_text_list('config/top_domains.txt')
        
        # Free mail domains (hardcoded for reliability)
        self.free_mail_domains = {
            'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 
            'yahoo.com', 'yahoo.co.uk', 'icloud.com', 'protonmail.com',
            'aol.com', 'mail.com', 'yandex.com', 'zoho.com'
        }
    
    def _load_text_list(self, filepath: str) -> Set[str]:
        """Load set of strings from text file"""
        try:
            with open(filepath, 'r') as f:
                return {line.strip().lower() for line in f if line.strip()}
        except FileNotFoundError:
            return set()
    
    def _get_default_typo_maps(self) -> Dict[str, Dict[str, str]]:
        """Default typo correction maps - only obvious typos, not legitimate TLDs"""
        return {
            "tld_typos": {
                # Only obvious typos, not legitimate country codes like .co (Colombia)
                ".con": ".com",
                ".cmo": ".com", 
                ".cim": ".com",
                ".c0m": ".com",
                ".comm": ".com",
                ".comn": ".com",
                ".vom": ".com",
                ".xom": ".com",
                ".dom": ".com",
                ".ocm": ".com",
                ".nett": ".net",
                ".ne": ".net",
                ".nte": ".net",
                ".orgg": ".org",
                ".ogr": ".org",
                ".rog": ".org"
            },
            "domain_typos": {
                "gmial.com": "gmail.com",
                "gamil.com": "gmail.com",
                "gmai.com": "gmail.com",
                "outlok.com": "outlook.com",
                "hotmial.com": "hotmail.com",
                "yahho.com": "yahoo.com",
                "yahooo.com": "yahoo.com",
                "faceboook.com": "facebook.com",
                "goolge.com": "google.com",
                "googel.com": "google.com"
            }
        }
    
    def extract_features(self, email: str) -> Dict[str, Any]:
        """
        Extract comprehensive features for an email address
        Returns dict with normalized email, flags, and metadata
        """
        features = {
            'original_email': email,
            'normalized_email': email,
            'flags': {},
            'syntax_valid': True,
            'local_part': '',
            'domain_part': '',
            'suggested_corrections': []
        }
        
        try:
            # Basic parsing
            if '@' not in email:
                features['flags']['missing_at'] = True
                features['syntax_valid'] = False
                return features
            
            parts = email.split('@')
            if len(parts) != 2:
                features['flags']['multiple_at'] = True
                features['syntax_valid'] = False
                return features
            
            local, domain = parts
            features['local_part'] = local
            features['domain_part'] = domain
            
            # Validate basic structure
            if not local or not domain:
                features['flags']['empty_parts'] = True
                features['syntax_valid'] = False
                return features
            
            # Extract individual feature categories
            self._extract_syntax_features(features)
            self._extract_normalization_features(features)
            self._extract_domain_features(features)
            self._extract_risk_features(features)
            self._extract_correction_suggestions(features)
            
        except Exception as e:
            features['flags']['extraction_error'] = True
            features['syntax_valid'] = False
        
        return features
    
    def _extract_syntax_features(self, features: Dict[str, Any]):
        """Extract syntax validation features"""
        local = features['local_part']
        domain = features['domain_part']
        
        # Local part validation
        if len(local) > 64:
            features['flags']['local_too_long'] = True
            features['syntax_valid'] = False
        
        if local.startswith('.') or local.endswith('.'):
            features['flags']['local_dot_boundaries'] = True
            features['syntax_valid'] = False
        
        if '..' in local:
            features['flags']['local_consecutive_dots'] = True
        
        if re.search(r'[^a-zA-Z0-9.!#$%&\'*+/=?^_`{|}~-]', local):
            features['flags']['local_invalid_chars'] = True
            features['syntax_valid'] = False
        
        # Domain part validation
        if len(domain) > 253:
            features['flags']['domain_too_long'] = True
            features['syntax_valid'] = False
        
        if domain.startswith('.') or domain.endswith('.'):
            features['flags']['domain_dot_boundaries'] = True
            features['syntax_valid'] = False
        
        if '..' in domain:
            features['flags']['domain_consecutive_dots'] = True
            features['syntax_valid'] = False
        
        # Domain labels validation
        labels = domain.split('.')
        for label in labels:
            if len(label) > 63:
                features['flags']['label_too_long'] = True
                features['syntax_valid'] = False
            
            if label.startswith('-') or label.endswith('-'):
                features['flags']['label_hyphen_boundaries'] = True
                features['syntax_valid'] = False
    
    def _extract_normalization_features(self, features: Dict[str, Any]):
        """Extract normalization and cleanup features"""
        email = features['original_email']
        
        # Detect various formatting issues
        if email != email.strip():
            features['flags']['has_whitespace'] = True
        
        if '\u200b' in email or '\u200c' in email or '\u200d' in email:
            features['flags']['has_zero_width'] = True
        
        if '"' in email or '"' in email or '"' in email:
            features['flags']['has_smart_quotes'] = True
        
        if '<' in email and '>' in email:
            features['flags']['has_angle_brackets'] = True
        
        if '＠' in email:
            features['flags']['has_fullwidth_at'] = True
        
        # Check for internal whitespace
        if ' ' in email.replace(' ', '').strip():
            features['flags']['has_internal_space'] = True
    
    def _extract_domain_features(self, features: Dict[str, Any]):
        """Extract domain-specific features"""
        domain = features['domain_part'].lower()
        
        # IDN/Punycode handling
        try:
            if any(ord(char) > 127 for char in domain):
                features['flags']['non_ascii_domain'] = True
                punycode_domain = idna.encode(domain, uts46=True).decode('ascii')
                features['punycode_domain'] = punycode_domain
        except (idna.core.IDNAError, UnicodeError):
            features['flags']['idna_error'] = True
            features['syntax_valid'] = False
        
        # TLD validation
        try:
            tld_info = tld.get_tld(f"http://{domain}", as_object=True)
            features['tld'] = tld_info.tld
            features['domain_without_tld'] = tld_info.domain
        except tld.exceptions.TldDomainNotFound:
            features['flags']['invalid_tld'] = True
            features['syntax_valid'] = False
        except Exception:
            features['flags']['tld_check_error'] = True
        
        # Domain categorization
        if domain in self.free_mail_domains:
            features['flags']['free_mail_domain'] = True
        
        if domain in self.disposable_domains:
            features['flags']['disposable_domain'] = True
        
        # Confusables detection
        if self._has_confusables(domain):
            features['flags']['unicode_confusable'] = True
    
    def _extract_risk_features(self, features: Dict[str, Any]):
        """Extract risk assessment features"""
        local = features['local_part'].lower()
        
        # Role account detection
        if local in self.role_locals:
            features['flags']['role_account'] = True
        
        # Common role prefixes
        role_prefixes = ['admin', 'info', 'sales', 'support', 'noreply', 'no-reply']
        if any(local.startswith(prefix) for prefix in role_prefixes):
            features['flags']['role_account'] = True
        
        # Suspicious patterns
        if len(set(local)) < 3:  # Very low character diversity
            features['flags']['low_diversity'] = True
        
        if re.match(r'^(test|temp|example|sample)', local):
            features['flags']['test_email'] = True
    
    def _extract_correction_suggestions(self, features: Dict[str, Any]):
        """Extract potential correction suggestions"""
        domain = features['domain_part'].lower()
        
        # TLD typo corrections
        for typo, correction in self.typo_maps.get('tld_typos', {}).items():
            if domain.endswith(typo):
                corrected_domain = domain[:-len(typo)] + correction
                features['suggested_corrections'].append({
                    'type': 'tld_typo',
                    'original': domain,
                    'suggested': corrected_domain,
                    'confidence': 0.95
                })
        
        # Domain typo corrections
        if domain in self.typo_maps.get('domain_typos', {}):
            corrected_domain = self.typo_maps['domain_typos'][domain]
            features['suggested_corrections'].append({
                'type': 'domain_typo',
                'original': domain,
                'suggested': corrected_domain,
                'confidence': 0.90
            })
    
    def _has_confusables(self, text: str) -> bool:
        """Check for Unicode confusable characters"""
        # Simplified confusables detection
        confusable_patterns = [
            # Mixed scripts
            (r'[а-я]', r'[a-z]'),  # Cyrillic + Latin
            (r'[α-ω]', r'[a-z]'),  # Greek + Latin
        ]
        
        for pattern1, pattern2 in confusable_patterns:
            if re.search(pattern1, text) and re.search(pattern2, text):
                return True
        
        return False
