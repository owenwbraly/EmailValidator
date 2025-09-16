"""
Deterministic Decision Engine for Email Validation
Makes decisions based on features, normalization, and deterministic rules
LLM processing is optional and only for 'review' cases
"""

from typing import Dict, Any, Optional, List
from .features import FeatureExtractor
from .normalize import EmailNormalizer


class DeterministicDecisionEngine:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.confidence_threshold = options.get('confidence_threshold', 0.85)
        self.exclude_role_accounts = options.get('exclude_role_accounts', True)
        
        # Initialize components
        self.feature_extractor = FeatureExtractor()
        self.normalizer = EmailNormalizer()
    
    def process_email(self, original_email: str) -> Dict[str, Any]:
        """
        Process email with deterministic-first approach
        Returns decision dict with action, output_email, confidence, changed, reason
        """
        # Step 1: Extract features
        features = self.feature_extractor.extract_features(original_email)
        
        # Step 2: Apply normalization
        normalized_email = self.normalizer.normalize_email(original_email, features)
        features['normalized_email'] = normalized_email
        
        # Step 3: Make deterministic decision
        decision = self._make_deterministic_decision(original_email, normalized_email, features)
        
        return decision
    
    def _make_deterministic_decision(self, original_email: str, normalized_email: str, 
                                   features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make deterministic decision based on features and rules
        Returns: accept, fix_auto, review, suppress
        """
        flags = features.get('flags', {})
        
        # Step 1: Check for immediate suppression conditions
        suppression_reason = self._check_suppression_conditions(features)
        if suppression_reason:
            return {
                'action': 'remove',
                'output_email': original_email,
                'confidence': 1.0,
                'changed': False,
                'reason': suppression_reason,
                'features': features
            }
        
        # Step 2: Check for high-confidence automatic fixes
        fix_result = self._check_auto_fixes(original_email, normalized_email, features)
        if fix_result:
            return fix_result
        
        # Step 3: Check for acceptance conditions
        if self._is_safe_to_accept(features):
            return {
                'action': 'accept',
                'output_email': normalized_email,
                'confidence': 0.95,
                'changed': normalized_email != original_email,
                'reason': 'Deterministic accept - valid syntax and safe domain',
                'features': features
            }
        
        # Step 4: Risky or ambiguous cases need review
        risk_flags = self._get_risk_flags(features)
        if risk_flags:
            return {
                'action': 'review',
                'output_email': normalized_email,
                'confidence': 0.5,
                'changed': normalized_email != original_email,
                'reason': f'Needs review - risk flags: {", ".join(risk_flags)}',
                'features': features
            }
        
        # Step 5: Default to review for unknown cases
        return {
            'action': 'review',
            'output_email': normalized_email,
            'confidence': 0.3,
            'changed': normalized_email != original_email,
            'reason': 'Deterministic review - insufficient confidence for auto-decision',
            'features': features
        }
    
    def _check_suppression_conditions(self, features: Dict[str, Any]) -> Optional[str]:
        """
        Check for conditions that mandate suppression
        Returns reason string if suppression required, None otherwise
        """
        flags = features.get('flags', {})
        
        # Invalid syntax - hard failures
        if not features.get('syntax_valid', True):
            syntax_issues = []
            if flags.get('missing_at'): syntax_issues.append('missing @')
            if flags.get('multiple_at'): syntax_issues.append('multiple @')
            if flags.get('empty_parts'): syntax_issues.append('empty parts')
            if flags.get('local_too_long'): syntax_issues.append('local too long')
            if flags.get('domain_too_long'): syntax_issues.append('domain too long')
            if flags.get('local_dot_boundaries'): syntax_issues.append('local dot boundaries')
            if flags.get('domain_dot_boundaries'): syntax_issues.append('domain dot boundaries')
            if flags.get('local_invalid_chars'): syntax_issues.append('invalid characters')
            
            if syntax_issues:
                return f"Invalid syntax: {', '.join(syntax_issues)}"
        
        # Invalid/dangerous domains
        if flags.get('invalid_tld'):
            return "Invalid TLD"
        
        if flags.get('idna_error'):
            return "Domain encoding error"
        
        # Policy-based exclusions
        if flags.get('disposable_domain'):
            return "Disposable domain"
        
        if self.exclude_role_accounts and flags.get('role_account'):
            return "Role account (excluded by policy)"
        
        # Security risks
        if flags.get('unicode_confusable'):
            return "Dangerous Unicode confusables"
        
        # Obviously fake/test emails
        if flags.get('test_email'):
            return "Test/example email"
        
        return None
    
    def _check_auto_fixes(self, original_email: str, normalized_email: str, 
                         features: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Check for high-confidence automatic fixes
        Returns decision dict if auto-fix available, None otherwise
        """
        corrections = features.get('suggested_corrections', [])
        
        # Apply high-confidence typo corrections
        for correction in corrections:
            if correction['confidence'] >= 0.90:
                if correction['type'] in ['tld_typo', 'domain_typo']:
                    local, domain = original_email.rsplit('@', 1)
                    if domain.lower() == correction['original']:
                        fixed_email = f"{local}@{correction['suggested']}"
                        return {
                            'action': 'fix_auto',
                            'output_email': fixed_email,
                            'confidence': correction['confidence'],
                            'changed': True,
                            'reason': f'Auto-fix {correction["type"]}: {correction["original"]} → {correction["suggested"]}',
                            'features': features
                        }
        
        # Basic normalization fixes
        if normalized_email != original_email:
            # Check if normalization fixed significant issues
            fixed_issues = []
            flags = features.get('flags', {})
            
            if flags.get('has_whitespace'): fixed_issues.append('whitespace')
            if flags.get('has_zero_width'): fixed_issues.append('zero-width chars')
            if flags.get('has_smart_quotes'): fixed_issues.append('smart quotes')
            if flags.get('has_angle_brackets'): fixed_issues.append('angle brackets')
            if flags.get('has_fullwidth_at'): fixed_issues.append('fullwidth @')
            
            if fixed_issues and self._is_safe_to_accept(features):
                return {
                    'action': 'fix_auto',
                    'output_email': normalized_email,
                    'confidence': 0.90,
                    'changed': True,
                    'reason': f'Auto-fix normalization: {", ".join(fixed_issues)}',
                    'features': features
                }
        
        return None
    
    def _is_safe_to_accept(self, features: Dict[str, Any]) -> bool:
        """
        Check if email is safe to accept automatically
        """
        flags = features.get('flags', {})
        
        # Must have valid syntax
        if not features.get('syntax_valid', True):
            return False
        
        # Must not have serious risk flags
        risk_flags = [
            'invalid_tld', 'idna_error', 'disposable_domain', 
            'unicode_confusable', 'test_email'
        ]
        
        if any(flags.get(flag) for flag in risk_flags):
            return False
        
        # Role accounts need policy check
        if flags.get('role_account') and self.exclude_role_accounts:
            return False
        
        # Must have reasonable domain
        if not features.get('tld') and not flags.get('free_mail_domain'):
            return False
        
        return True
    
    def _get_risk_flags(self, features: Dict[str, Any]) -> List[str]:
        """Get list of risk flags present in features"""
        flags = features.get('flags', {})
        risk_flags = []
        
        # Structural risks
        if flags.get('domain_consecutive_dots'):
            risk_flags.append('domain_consecutive_dots')
        
        if flags.get('local_consecutive_dots'):
            risk_flags.append('local_consecutive_dots')
        
        if flags.get('label_too_long'):
            risk_flags.append('label_too_long')
        
        if flags.get('label_hyphen_boundaries'):
            risk_flags.append('label_hyphen_boundaries')
        
        # Content risks
        if flags.get('low_diversity'):
            risk_flags.append('low_diversity')
        
        if flags.get('non_ascii_domain'):
            risk_flags.append('non_ascii_domain')
        
        # Policy-related flags (not automatically excluding)
        if flags.get('role_account') and not self.exclude_role_accounts:
            risk_flags.append('role_account')
        
        return risk_flags
    
    def get_canonical_key(self, email: str, provider_aware: bool = True) -> str:
        """
        Get canonical form for de-duplication
        """
        return self.normalizer.get_canonical_form(email, provider_aware)
    
    def generate_canonical_key(self, email: str, provider_aware: bool = True) -> str:
        """
        Generate canonical key for de-duplication (alias for compatibility)
        """
        return self.get_canonical_key(email, provider_aware)