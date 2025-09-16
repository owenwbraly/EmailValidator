"""
Deterministic Decision Engine for Email Validation
Makes decisions based on features, normalization, and deterministic rules
LLM processing is optional and only for 'review' cases
"""

from typing import Dict, Any, Optional, List
from .deterministic_email_engine import validate_email_deterministic, load_reference_sets, canonical_key


class DeterministicDecisionEngine:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.confidence_threshold = options.get('confidence_threshold', 0.85)
        self.exclude_role_accounts = options.get('exclude_role_accounts', True)
        
        # Load reference sets for the new deterministic engine
        reference_sets = load_reference_sets()
        
        # Type cast reference sets for proper typing (handle potential None/object values)
        disposable = reference_sets.get('disposable_set', set())
        self.disposable_set = disposable if isinstance(disposable, set) else set(disposable) if disposable else set()
        
        roles = reference_sets.get('role_locals', set()) 
        self.role_locals = roles if isinstance(roles, set) else set(roles) if roles else set()
        
        domains = reference_sets.get('top_domains', [])
        self.top_domains = domains if isinstance(domains, list) else list(domains) if domains else []
        
        tlds = reference_sets.get('tld_whitelist')
        self.tld_whitelist = tlds if isinstance(tlds, set) else set(tlds) if tlds else None
    
    def process_email(self, original_email: str) -> Dict[str, Any]:
        """
        Process email with new deterministic engine
        Returns decision dict with action, output_email, confidence, changed, reason
        """
        # Use the new deterministic engine
        result = validate_email_deterministic(
            original_email,
            exclude_role_accounts=self.exclude_role_accounts,
            disposable_set=self.disposable_set,
            role_locals=self.role_locals,
            top_domains=self.top_domains,
            tld_whitelist=self.tld_whitelist
        )
        
        # Map new engine results to expected format
        return self._map_result_to_legacy_format(original_email, result)
    
    def _map_result_to_legacy_format(self, original_email: str, result) -> Dict[str, Any]:
        """
        Map new deterministic engine result to legacy format expected by pipeline
        """
        # Map actions: suppress -> remove for compatibility
        action = result.action
        if action == 'suppress':
            action = 'remove'
        elif action == 'fix_auto':
            action = 'fix_auto'
        
        # Determine output email and if it changed
        output_email = result.suggested_fix if result.suggested_fix else result.normalized_email
        changed = output_email != original_email
        
        # Create features dict for compatibility
        features = {
            'risk_reasons': result.risk_reasons,
            'canonical_key': result.canonical_key,
            'suggested_fix': result.suggested_fix,
            'normalized_email': result.normalized_email
        }
        
        return {
            'action': action,
            'output_email': output_email,
            'confidence': result.confidence,
            'changed': changed,
            'reason': result.notes or f'{action} - {result.confidence:.1%} confidence',
            'features': features
        }
    
    
    
    
    
    def get_canonical_key(self, email: str, provider_aware: bool = True) -> str:
        """
        Get canonical form for de-duplication using new engine
        """
        return canonical_key(email, provider_aware) or email.lower()
    
    def generate_canonical_key(self, email: str, provider_aware: bool = True) -> str:
        """
        Generate canonical key for de-duplication (alias for compatibility)
        """
        return self.get_canonical_key(email, provider_aware)