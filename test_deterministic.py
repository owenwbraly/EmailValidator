#!/usr/bin/env python3

# Quick test of the deterministic engine
from app.deterministic_email_engine import validate_email_deterministic, load_reference_sets

# Load reference sets
ref_sets = load_reference_sets()

test_emails = [
    "john@gmial.com",  # Should suggest gmail.com
    "alice@hotnail.com",  # Should suggest hotmail.com
    "jane@example.com",  # Should accept
    "test@example.con"   # Should suggest .com
]

print("Testing deterministic engine:")
print("=" * 50)

for email in test_emails:
    result = validate_email_deterministic(
        email,
        exclude_role_accounts=False,
        disposable_set=ref_sets.get('disposable_set', set()),
        role_locals=ref_sets.get('role_locals', set()),
        top_domains=ref_sets.get('top_domains', []),
        tld_whitelist=ref_sets.get('tld_whitelist')
    )
    
    print(f"Email: {email}")
    print(f"  Action: {result.action}")
    print(f"  Normalized: {result.normalized_email}")
    print(f"  Suggested fix: {result.suggested_fix}")
    print(f"  Confidence: {result.confidence}")
    print(f"  Canonical key: {result.canonical_key}")
    print(f"  Risk reasons: {result.risk_reasons}")
    print(f"  Notes: {result.notes}")
    print()