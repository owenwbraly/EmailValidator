"""
Email routing and decision making utilities
Implements the authoritative decision policy for email processing
"""

from typing import Dict, Any, Optional, Tuple


class DecisionRouter:
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.confidence_threshold = options.get('confidence_threshold', 0.85)
        self.exclude_role_accounts = options.get('exclude_role_accounts', True)
    
    def decide(self, original_email: str, normalized_email: str, 
              llm_result: Dict[str, Any], features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make authoritative decision about email processing
        Returns dict with action, output_email, confidence, changed, reason
        """
        
        # Extract LLM recommendation
        llm_action = llm_result.get('action', 'review')
        llm_confidence = float(llm_result.get('confidence', 0.0))
        suggested_fix = llm_result.get('suggested_fix')
        
        # Check for immediate removal conditions
        removal_reason = self._check_removal_conditions(features, llm_result)
        if removal_reason:
            return {
                'action': 'remove',
                'output_email': original_email,
                'confidence': llm_confidence,
                'changed': False,
                'reason': removal_reason
            }
        
        # Apply decision logic
        if llm_action == 'fix_auto' and llm_confidence >= self.confidence_threshold:
            # High-confidence fix
            output_email = suggested_fix or llm_result.get('normalized_email', normalized_email)
            return {
                'action': 'fix',
                'output_email': output_email,
                'confidence': llm_confidence,
                'changed': output_email != original_email,
                'reason': f'LLM fix (confidence: {llm_confidence:.2f})'
            }
        
        elif llm_action == 'accept':
            # Accept with normalization
            output_email = llm_result.get('normalized_email', normalized_email)
            return {
                'action': 'accept',
                'output_email': output_email,
                'confidence': llm_confidence,
                'changed': output_email != original_email,
                'reason': f'LLM accept (confidence: {llm_confidence:.2f})'
            }
        
        elif llm_action == 'suppress':
            # LLM recommends suppression
            return {
                'action': 'remove',
                'output_email': original_email,
                'confidence': llm_confidence,
                'changed': False,
                'reason': f'LLM suppress (confidence: {llm_confidence:.2f})'
            }
        
        else:
            # Review or low confidence - check if risky
            risky_flags = self._get_risky_flags(features)
            
            if llm_action == 'review' and llm_confidence < self.confidence_threshold and risky_flags:
                return {
                    'action': 'remove',
                    'output_email': original_email,
                    'confidence': llm_confidence,
                    'changed': False,
                    'reason': f'Low confidence review with risks: {", ".join(risky_flags)}'
                }
            else:
                # Conservative default - accept normalized
                output_email = normalized_email
                return {
                    'action': 'accept',
                    'output_email': output_email,
                    'confidence': llm_confidence,
                    'changed': output_email != original_email,
                    'reason': f'Conservative accept (confidence: {llm_confidence:.2f})'
                }
    
    def _check_removal_conditions(self, features: Dict[str, Any], llm_result: Dict[str, Any]) -> Optional[str]:
        """
        Check for conditions that mandate removal
        Returns reason string if removal required, None otherwise
        """
        flags = features.get('flags', {})
        
        # Invalid syntax
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
        
        # Invalid/implausible domain
        if flags.get('invalid_tld'):
            return "Invalid TLD"
        
        if flags.get('idna_error'):
            return "Domain encoding error"
        
        # Disposable domain
        if flags.get('disposable_domain'):
            return "Disposable domain"
        
        # Role accounts (if exclusion enabled)
        if self.exclude_role_accounts and flags.get('role_account'):
            return "Role account (excluded by policy)"
        
        # Dangerous confusables
        if flags.get('unicode_confusable'):
            return "Dangerous Unicode confusables"
        
        # Test/junk emails
        if flags.get('test_email') and llm_result.get('confidence', 0) > 0.8:
            return "Test/example email"
        
        return None
    
    def _get_risky_flags(self, features: Dict[str, Any]) -> list:
        """Get list of risky flags present in features"""
        flags = features.get('flags', {})
        risky_flags = []
        
        if flags.get('invalid_tld'):
            risky_flags.append('invalid_tld')
        
        if flags.get('disposable_domain'):
            risky_flags.append('disposable_domain')
        
        if flags.get('unicode_confusable'):
            risky_flags.append('unicode_confusable')
        
        if flags.get('domain_consecutive_dots'):
            risky_flags.append('domain_consecutive_dots')
        
        if flags.get('local_consecutive_dots'):
            risky_flags.append('local_consecutive_dots')
        
        return risky_flags
