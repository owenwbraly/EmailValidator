"""
LLM adapter for email classification
Supports OpenAI and Anthropic with JSON schema enforcement
"""

import json
import os
import time
import logging
from typing import Dict, Any, Optional
import openai
import anthropic
from anthropic import Anthropic


class LLMAdapter:
    def __init__(self, config: Dict[str, Any]):
        self.provider = config['provider'].lower()
        self.model = config['model']
        
        # Initialize clients
        if self.provider == 'openai':
            self.client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        elif self.provider == 'anthropic':
            self.client = Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
        
        # JSON schema for validation
        self.expected_schema = {
            "input_email": "",
            "normalized_email": "",
            "action": "",
            "confidence": 0.0,
            "risk_reasons": [],
            "suggested_fix": None,
            "notes": ""
        }
        
        # Rate limiting
        self.last_call_time = 0
        self.min_interval = 0.1  # 100ms between calls
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 1.0
    
    def classify_email(self, email: str, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Classify email using LLM with JSON schema enforcement
        Returns standardized classification result
        """
        # Rate limiting
        self._rate_limit()
        
        # Prepare request data
        request_data = self._prepare_request(email, features)
        
        # Try classification with retries
        for attempt in range(self.max_retries):
            try:
                if self.provider == 'openai':
                    result = self._call_openai(request_data)
                else:  # anthropic
                    result = self._call_anthropic(request_data)
                
                # Validate and return
                return self._validate_response(result, email)
                
            except Exception as e:
                logging.warning(f"LLM call attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    # Fallback to deterministic decision
                    return self._fallback_decision(email, features)
    
    def _prepare_request(self, email: str, features: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare request data for LLM"""
        flags = features.get('flags', {})
        
        # Convert boolean flags to simple object
        flag_summary = {
            'syntax_issues': not features.get('syntax_valid', True),
            'role_account': flags.get('role_account', False),
            'disposable_domain': flags.get('disposable_domain', False),
            'free_mail_domain': flags.get('free_mail_domain', False),
            'non_ascii_domain': flags.get('non_ascii_domain', False),
            'unicode_confusable': flags.get('unicode_confusable', False),
            'test_email': flags.get('test_email', False),
            'has_suggestions': len(features.get('suggested_corrections', [])) > 0
        }
        
        return {
            'email': email,
            'flags': flag_summary,
            'suggestions': features.get('suggested_corrections', [])
        }
    
    def _call_openai(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Make OpenAI API call with JSON mode"""
        # the newest OpenAI model is "gpt-5" which was released August 7, 2025.
        # do not change this unless explicitly requested by the user
        
        system_prompt = self._get_system_prompt()
        user_prompt = self._get_user_prompt(request_data)
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.1,
            max_tokens=500
        )
        
        content = response.choices[0].message.content
        return json.loads(content)
    
    def _call_anthropic(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Make Anthropic API call with JSON enforcement"""
        # The newest Anthropic model is "claude-sonnet-4-20250514", not "claude-3-7-sonnet-20250219", "claude-3-5-sonnet-20241022" nor "claude-3-sonnet-20240229". 
        
        system_prompt = self._get_system_prompt()
        user_prompt = self._get_user_prompt(request_data)
        
        response = self.client.messages.create(
            model=self.model,
            max_tokens=500,
            temperature=0.1,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_prompt}
            ]
        )
        
        content = response.content[0].text
        return json.loads(content)
    
    def _get_system_prompt(self) -> str:
        """Get system prompt for LLM"""
        return """You validate email plausibility. Do not claim deliverability or use the network. Consider provided flags (syntax errors, TLD/domain risks, role/disposable, IDN, confusables).

Return only the JSON contract. If a safe, obvious repair exists, propose exactly one suggested_fix. Otherwise prefer review/suppress over guessing.

Your response must be valid JSON matching this exact schema:
{
  "input_email": "original email",
  "normalized_email": "cleaned version", 
  "action": "accept|fix_auto|review|suppress",
  "confidence": 0.0,
  "risk_reasons": ["list", "of", "risks"],
  "suggested_fix": "fixed_email@domain.com or null",
  "notes": "brief explanation"
}

Actions:
- accept: email is valid/plausible as-is
- fix_auto: confident correction available
- review: uncertain, needs human review
- suppress: clearly invalid/risky

Confidence scale: 0.0 (uncertain) to 1.0 (certain)"""
    
    def _get_user_prompt(self, request_data: Dict[str, Any]) -> str:
        """Get user prompt with email and flags"""
        email = request_data['email']
        flags = request_data['flags']
        suggestions = request_data['suggestions']
        
        prompt = f"Email: {email}\n\nFlags: {json.dumps(flags, indent=2)}"
        
        if suggestions:
            prompt += f"\n\nSuggested corrections: {json.dumps(suggestions, indent=2)}"
        
        prompt += "\n\nReturn JSON classification:"
        
        return prompt
    
    def _validate_response(self, response: Dict[str, Any], original_email: str) -> Dict[str, Any]:
        """Validate LLM response against schema"""
        # Check required fields
        required_fields = ['action', 'confidence']
        for field in required_fields:
            if field not in response:
                raise ValueError(f"Missing required field: {field}")
        
        # Validate action
        valid_actions = ['accept', 'fix_auto', 'review', 'suppress']
        if response['action'] not in valid_actions:
            response['action'] = 'review'
        
        # Validate confidence
        try:
            confidence = float(response['confidence'])
            response['confidence'] = max(0.0, min(1.0, confidence))
        except (ValueError, TypeError):
            response['confidence'] = 0.5
        
        # Ensure input_email is set
        if 'input_email' not in response:
            response['input_email'] = original_email
        
        # Ensure normalized_email is set
        if 'normalized_email' not in response:
            response['normalized_email'] = original_email
        
        # Validate suggested_fix
        if 'suggested_fix' in response and response['suggested_fix'] == '':
            response['suggested_fix'] = None
        
        # Ensure risk_reasons is list
        if 'risk_reasons' not in response:
            response['risk_reasons'] = []
        elif not isinstance(response['risk_reasons'], list):
            response['risk_reasons'] = []
        
        # Limit notes length
        if 'notes' in response and len(response['notes']) > 120:
            response['notes'] = response['notes'][:117] + '...'
        
        return response
    
    def _fallback_decision(self, email: str, features: Dict[str, Any]) -> Dict[str, Any]:
        """Fallback decision when LLM fails"""
        flags = features.get('flags', {})
        
        # Determine action based on deterministic flags
        if not features.get('syntax_valid', True):
            action = 'suppress'
            confidence = 0.9
            reasons = ['syntax_invalid']
        elif flags.get('disposable_domain'):
            action = 'suppress'
            confidence = 0.85
            reasons = ['disposable_domain']
        elif flags.get('role_account'):
            action = 'review'
            confidence = 0.7
            reasons = ['role_account']
        elif features.get('suggested_corrections'):
            action = 'fix_auto'
            confidence = 0.8
            reasons = ['has_corrections']
        else:
            action = 'accept'
            confidence = 0.6
            reasons = []
        
        return {
            'input_email': email,
            'normalized_email': email,
            'action': action,
            'confidence': confidence,
            'risk_reasons': reasons,
            'suggested_fix': None,
            'notes': 'Fallback decision (LLM unavailable)'
        }
    
    def _rate_limit(self):
        """Simple rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_call_time
        
        if time_since_last < self.min_interval:
            time.sleep(self.min_interval - time_since_last)
        
        self.last_call_time = time.time()
