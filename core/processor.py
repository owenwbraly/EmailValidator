"""
Array-based email processing with deterministic cleaning
Processes arrays of EmailEntry objects efficiently
"""

from typing import List, Dict, Any
from models.EmailEntry import EmailEntry
from .email_hygeine_engine import validate_email_deterministic, load_reference_sets, canonical_key


class EmailProcessor:
    """
    Processes arrays of EmailEntry objects using deterministic engine
    """
    
    def __init__(self, options: Dict[str, Any]):
        self.options = options
        self.exclude_role_accounts = options.get('exclude_role_accounts', True)
        
        # Load reference sets for the deterministic engine
        reference_sets = load_reference_sets()
        
        # Type cast reference sets for proper typing
        disposable = reference_sets.get('disposable_set', set())
        self.disposable_set = disposable if isinstance(disposable, set) else set(disposable) if disposable else set()
        
        roles = reference_sets.get('role_locals', set()) 
        self.role_locals = roles if isinstance(roles, set) else set(roles) if roles else set()
        
        domains = reference_sets.get('top_domains', [])
        self.top_domains = domains if isinstance(domains, list) else list(domains) if domains else []
        
        tlds = reference_sets.get('tld_whitelist')
        self.tld_whitelist = tlds if isinstance(tlds, set) else set(tlds) if tlds else None
    
    def process_email_entries(self, entries: List[EmailEntry]) -> Dict[str, Any]:
        """
        Process all email entries using deterministic engine
        Updates entries in-place with cleaned values and metadata
        """
        results = {
            'accepted': 0,
            'fixed': 0,
            'removed': 0,
            'total_processed': 0
        }
        
        changes_report = []
        rejected_report = []
        
        for entry in entries:
            if entry.is_empty():
                continue
                
            # Process with deterministic engine
            decision = self._process_email(entry.raw)
            
            # Update entry with results
            entry.cleaned = decision['output_email']
            entry.action = decision['action']
            entry.confidence = decision['confidence']
            entry.changed = decision['changed']
            entry.reason = decision['reason']
            entry.canonical_key = self._get_canonical_key(entry.cleaned)
            
            # Update counters
            if decision['action'] in ['accept', 'fix_auto']:
                if decision['changed']:
                    results['fixed'] += 1
                else:
                    results['accepted'] += 1
            elif decision['action'] == 'remove':
                results['removed'] += 1
                
                # Record rejected email
                rejected_report.append({
                    'sheet': entry.sheet,
                    'row_number': entry.row_number,
                    'col_number': entry.col_number,
                    'col_name': entry.col_name,
                    'original_email': entry.raw,
                    'reason': entry.reason,
                    'confidence': entry.confidence,
                    'action': 'removed'
                })
            
            results['total_processed'] += 1
            
            # Record changes
            if decision['changed']:
                changes_report.append({
                    'sheet': entry.sheet,
                    'row_number': entry.row_number,
                    'col_number': entry.col_number,
                    'col_name': entry.col_name,
                    'original_email': entry.raw,
                    'new_email': entry.cleaned,
                    'reason': entry.reason,
                    'confidence': entry.confidence
                })
        
        return {
            'results': results,
            'changes_report': changes_report,
            'rejected_report': rejected_report
        }
    
    def _process_email(self, original_email: str) -> Dict[str, Any]:
        """
        Process email with deterministic engine
        Returns decision dict with action, output_email, confidence, changed, reason
        """
        # Use the deterministic engine
        result = validate_email_deterministic(
            original_email,
            exclude_role_accounts=self.exclude_role_accounts,
            disposable_set=self.disposable_set,
            role_locals=self.role_locals,
            top_domains=self.top_domains,
            tld_whitelist=self.tld_whitelist
        )
        
        # Map results to expected format
        return self._map_result_to_format(original_email, result)
    
    def _map_result_to_format(self, original_email: str, result) -> Dict[str, Any]:
        """
        Map deterministic engine result to expected format
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
        
        return {
            'action': action,
            'output_email': output_email,
            'confidence': result.confidence,
            'changed': changed,
            'reason': result.notes or f'{action} - {result.confidence:.1%} confidence'
        }
    
    def _get_canonical_key(self, email: str, provider_aware: bool = True) -> str:
        """
        Get canonical form for de-duplication
        """
        return canonical_key(email, provider_aware) or email.lower()
    
    def update_dataframes_with_cleaned_emails(self, file_data: Dict[str, Any], 
                                            entries: List[EmailEntry]) -> Dict[str, Any]:
        """
        Update original DataFrames with cleaned email values
        Only updates non-duplicate entries
        """
        updated_data = {}
        
        for sheet_name, df in file_data.items():
            df_reset = df.reset_index(drop=True)
            updated_df = df_reset.copy()
            
            # Find entries for this sheet
            sheet_entries = [e for e in entries if e.sheet == sheet_name and not e.is_duplicate()]
            
            for entry in sheet_entries:
                if not entry.is_empty() and entry.action != 'remove':
                    # Update with cleaned email
                    row_idx = entry.row_number - 1  # Convert to 0-based
                    if 0 <= row_idx < len(updated_df):
                        updated_df.iloc[row_idx, updated_df.columns.get_loc(entry.col_name)] = entry.cleaned
                elif entry.action == 'remove':
                    # Remove the email but keep the row
                    row_idx = entry.row_number - 1  # Convert to 0-based
                    if 0 <= row_idx < len(updated_df):
                        updated_df.iloc[row_idx, updated_df.columns.get_loc(entry.col_name)] = ""
            
            updated_data[sheet_name] = updated_df
        
        return updated_data
