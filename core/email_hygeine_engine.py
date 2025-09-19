# engine.py
# ------------------------------------------------------------
# Elite deterministic validation for email hygiene & dedupe.
# - No network calls; no deliverability claims.
# - Pragmatic RFC checks + IDNA + TLD sanity + homoglyph flags.
# - Safe, single-step correction suggestions (TLD/domain typos, formatting).
# - Provider-aware canonicalization (e.g., Gmail ignores dots and +tags).
# - Returns a strict decision + reason codes + rule-based confidence.
# ------------------------------------------------------------

from __future__ import annotations
import re
import os
import json
import unicodedata
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple

# Optional fast fuzzy; falls back to difflib if not installed
try:
    from rapidfuzz import fuzz, process as rf_process
    _HAS_RAPIDFUZZ = True
except Exception:
    _HAS_RAPIDFUZZ = False
    import difflib

try:
    import idna  # robust IDN handling
except Exception as e:
    raise ImportError("Please `pip install idna` to enable IDN domain support") from e

# ----------------------------
# Config & reference datasets
# ----------------------------

DEFAULT_TLD_FIX = {
    ".con": ".com",
    ".cmo": ".com",
    ".cim": ".com",
    ".c0m": ".com",
    ".coom": ".com",
    ".nety": ".net",
    ".orgg": ".org",
}

DEFAULT_DOMAIN_FIX = {
    "gmial.com": "gmail.com",
    "gamil.com": "gmail.com",
    "gnail.com": "gmail.com",
    "hotnail.com": "hotmail.com",
    "yahho.com": "yahoo.com",
    "outlok.com": "outlook.com",
    "faceboook.com": "facebook.com",
    "icloud.con": "icloud.com",
}

# Large, evolving lists can be provided via local files:
# config/disposable_domains.txt, config/role_locals.txt, config/top_domains.txt
def _load_list(path: str) -> List[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip().lower() for ln in f if ln.strip() and not ln.startswith("#")]
    except FileNotFoundError:
        return []

# Small embedded fallbacks (you should extend via config files)
EMBED_FREE_MAIL = {
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com",
    "yahoo.com", "yahoo.co.uk", "icloud.com", "me.com", "proton.me", "protonmail.com",
    "aol.com", "gmx.com", "pm.me"
}

EMBED_ROLE_LOCAL_PARTS = {
    "admin","root","info","sales","support","help","contact","hello",
    "noreply","no-reply","donotreply","billing","accounts","careers",
    "jobs","press","marketing","newsletter","devnull","abuse","postmaster"
}

# Common Gmail-like providers that ignore dots and plus-tags
GMAIL_LIKE = {"gmail.com", "googlemail.com"}

# Suspicious unicode scripts to flag in domains
SUSPICIOUS_SCRIPTS = ("CYRILLIC", "GREEK", "CJK", "ARABIC", "HEBREW")

# Zero-width characters to strip
ZW = "".join(["\u200b", "\u200c", "\u200d", "\u2060"])
ZW_RE = re.compile(f"[{re.escape(ZW)}]")

# Smart quotes translation table - use replace() method instead of maketrans for multi-byte chars
SMART_QUOTES_MAP = {""": "\"", """: "\"", "'": "'", "'": "'"}

# Pragmatic RFC 5322-ish local-part (unquoted) char set
LOCAL_SAFE_CHARS = r"A-Za-z0-9!#$%&'*+/=?^_`{|}~\-"
LOCAL_UNQUOTED_RE = re.compile(rf"^[{LOCAL_SAFE_CHARS}](?:[{LOCAL_SAFE_CHARS}\.]*[{LOCAL_SAFE_CHARS}])?$")

# Domain label rules
DOMAIN_LABEL_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$")

# Risk weights → confidence mapping
RISK_WEIGHTS: Dict[str, float] = {
    "invalid_syntax": 0.9,
    "multiple_at": 0.7,
    "empty_local": 0.7,
    "empty_domain": 0.7,
    "leading_trailing_dot": 0.5,
    "double_dot_local": 0.5,
    "double_dot_domain": 0.5,
    "bad_tld": 0.8,
    "invalid_domain_label": 0.7,
    "unicode_confusable": 0.6,
    "non_ascii_domain": 0.3,
    "disposable_domain": 0.7,
    "role_account": 0.3,
    "free_mail_domain": 0.1,
    "whitespace_present": 0.2,
}

@dataclass
class DeterministicResult:
    input_email: str
    normalized_email: str
    action: str                       # accept | fix_auto | review | suppress
    confidence: float
    risk_reasons: List[str]
    suggested_fix: Optional[str]
    notes: str
    canonical_key: Optional[str]      # used for dedupe

    def as_dict(self) -> Dict:
        return asdict(self)

# ----------------------------
# Utility: script/charset flags
# ----------------------------

def _has_suspicious_script(s: str) -> bool:
    """Flag if any char is from a commonly spoofed script (domain labels only)."""
    for ch in s:
        if ord(ch) < 128:
            continue
        try:
            name = unicodedata.name(ch)
        except ValueError:
            # unnamed char → treat as suspicious
            return True
        if any(tag in name for tag in SUSPICIOUS_SCRIPTS):
            return True
    return False

def _is_ascii(s: str) -> bool:
    try:
        s.encode("ascii")
        return True
    except UnicodeEncodeError:
        return False

# ----------------------------
# Normalization & parsing
# ----------------------------

def normalize_email_raw(s: str) -> Tuple[str, List[str]]:
    """
    Fast normalization without altering semantics:
    - Strip whitespace, zero-width chars, angle brackets; replace smart quotes/fullwidth @.
    - Remove internal spaces; lowercase domain; strip trailing dot; collapse .. in domain.
    - Do not lowercase local part; do not remove plus tags.
    Returns (normalized_email, flags_applied)
    """
    flags: List[str] = []
    if not isinstance(s, str):
        return s, ["non_string"]
    original = s

    s = ZW_RE.sub("", s).strip()
    # Replace smart quotes
    for smart, regular in SMART_QUOTES_MAP.items():
        s = s.replace(smart, regular)
    if "<" in s or ">" in s:
        s = s.strip("<>").strip()
        flags.append("angle_brackets_stripped")

    if "＠" in s:
        s = s.replace("＠", "@")
        flags.append("fullwidth_at_replaced")

    if re.search(r"\s", s):
        s = re.sub(r"\s+", "", s)
        flags.append("whitespace_removed")

    if s.count("@") != 1:
        # let syntax validator handle; still return best-effort
        return s, flags

    local, domain = s.split("@", 1)
    domain = domain.rstrip(".")
    if domain != s.split("@",1)[1]:
        flags.append("trailing_dot_domain_stripped")

    # collapse domain consecutive dots
    if ".." in domain:
        domain = re.sub(r"\.{2,}", ".", domain)
        flags.append("double_dot_domain_collapsed")

    domain_lower = domain.lower()
    if domain_lower != domain:
        flags.append("domain_lowercased")
    s = f"{local}@{domain_lower}"

    if s != original and "whitespace_removed" not in flags:
        flags.append("normalized")

    return s, flags

def split_local_domain(email: str) -> Tuple[str, str]:
    if email.count("@") != 1:
        return "", ""
    return email.split("@", 1)

# ----------------------------
# Domain / TLD validation
# ----------------------------

def idna_ascii(domain: str) -> Tuple[str, List[str]]:
    """
    Convert potentially-unicode domain to ASCII punycode per IDNA.
    Returns ascii_domain and flags (non_ascii_domain; unicode_confusable).
    """
    flags: List[str] = []
    if not domain:
        return domain, flags
    if _is_ascii(domain):
        # still flag mixed scripts if present (should be ASCII here anyway)
        return domain, flags
    ascii_domain = ""
    try:
        ascii_domain = idna.encode(domain, uts46=True).decode("ascii")
        flags.append("non_ascii_domain")
    except idna.IDNAError:
        # invalid IDN → leave as-is; validator will mark invalid
        return domain, flags

    # Heuristic: flag suspicious scripts in original unicode form
    if _has_suspicious_script(domain):
        flags.append("unicode_confusable")

    return ascii_domain, flags

def validate_domain_structure(domain_ascii: str) -> List[str]:
    """
    Validate domain in ASCII (punycode if needed).
    - Labels 1..63 chars; allowed chars; no leading/trailing hyphen.
    - No empty labels; no consecutive dots (should be collapsed already).
    """
    risks: List[str] = []
    if not domain_ascii:
        return ["empty_domain"]
    labels = domain_ascii.split(".")
    if any(lbl == "" for lbl in labels):
        risks.append("double_dot_domain")
    for lbl in labels:
        if len(lbl) == 0 or len(lbl) > 63:
            risks.append("invalid_domain_label")
            continue
        if not DOMAIN_LABEL_RE.match(lbl):
            risks.append("invalid_domain_label")
    return risks

# Public suffix check: light heuristic if `tld` not installed
def is_known_tld(domain_ascii: str, known_tlds: Optional[set] = None) -> bool:
    """
    Naive TLD recognition using a small embedded set or user-provided list.
    For stronger checks, integrate `tld` or PSL. Here we keep it dependency-light.
    """
    if not domain_ascii or "." not in domain_ascii:
        return False
    tld = domain_ascii.rsplit(".", 1)[-1].lower()
    if known_tlds is None:
        # minimal whitelist; extend via config for production
        known_tlds = {
            "com","net","org","io","co","ai","gov","edu","us","uk","de","fr","es","it","nl",
            "ca","au","ch","se","no","dk","fi","pt","br","in","sg","jp","kr","cn","me","tv","dev"
        }
    return tld in known_tlds

# ----------------------------
# Local-part validation
# ----------------------------

def validate_local_part(local: str) -> List[str]:
    risks: List[str] = []
    if not local:
        return ["empty_local"]
    if local.startswith(".") or local.endswith("."):
        risks.append("leading_trailing_dot")
    if ".." in local:
        risks.append("double_dot_local")
    # quoted strings are complex; we treat quotes as risky unless fully quoted
    if local.startswith('"') and local.endswith('"'):
        # allow minimal quoted local; still risky for outreach
        return risks
    # unquoted path: pragmatic safe chars
    if not LOCAL_UNQUOTED_RE.match(local):
        risks.append("invalid_syntax")
    return risks

# ----------------------------
# Role / disposable / freemail flags
# ----------------------------

def classify_role_account(local: str, role_locals: Optional[set] = None) -> bool:
    if role_locals is None:
        role_locals = EMBED_ROLE_LOCAL_PARTS
    return local.lower() in role_locals

def classify_free_mail(domain_ascii: str) -> bool:
    return domain_ascii.lower() in EMBED_FREE_MAIL

def classify_disposable(domain_ascii: str, disposable_set: Optional[set] = None) -> bool:
    if disposable_set is None:
        disposable_set = set(_load_list(os.path.join("config","disposable_domains.txt")))
    return domain_ascii.lower() in disposable_set

# ----------------------------
# Correction suggestions
# ----------------------------

def suggest_tld_fix(domain_ascii: str) -> Optional[str]:
    for bad, good in DEFAULT_TLD_FIX.items():
        if domain_ascii.endswith(bad):
            return domain_ascii[:-len(bad)] + good
    return None

def suggest_exact_domain_fix(domain_ascii: str) -> Optional[str]:
    return DEFAULT_DOMAIN_FIX.get(domain_ascii)

def suggest_fuzzy_domain_fix(domain_ascii: str, top_domains: Optional[List[str]] = None,
                             min_score: int = 90) -> Optional[str]:
    if top_domains is None:
        top_domains = _load_list(os.path.join("config","top_domains.txt"))
    if not top_domains:
        return None
    cand = None
    if _HAS_RAPIDFUZZ:
        match = rf_process.extractOne(domain_ascii, top_domains, scorer=fuzz.QRatio)
        if match and match[1] >= min_score:
            cand = match[0]
    else:
        # difflib fallback
        matches = difflib.get_close_matches(domain_ascii, top_domains, n=1, cutoff=min_score/100.0)
        cand = matches[0] if matches else None
    if cand and len(cand) > 2 and abs(len(cand) - len(domain_ascii)) <= 2:
        return cand
    return None

def collapse_local_double_dot(local: str) -> Optional[str]:
    if ".." in local:
        return re.sub(r"\.{2,}", ".", local)
    return None

# ----------------------------
# Canonicalization for de-dup
# ----------------------------

def canonical_key(email: str, provider_aware: bool = True) -> Optional[str]:
    """
    Build a canonical key for deduplication:
    - Domain: IDNA→ASCII→lowercase.
    - Local: lowercase for freemail; for Gmail-like providers, remove dots and strip +tag.
    - Strip zero-width and confusables in key space (doesn't touch display email).
    Returns None if syntax is irrecoverably broken.
    """
    norm, _ = normalize_email_raw(email)
    if norm.count("@") != 1:
        return None
    local, domain = split_local_domain(norm)
    domain_ascii, _ = idna_ascii(domain)
    d = domain_ascii.lower()
    l = local
    if provider_aware and d in GMAIL_LIKE:
        # Gmail ignores dots and treats anything after '+' as a tag
        l = l.split("+", 1)[0].replace(".", "")
    else:
        l = l.lower()
    l = ZW_RE.sub("", l)
    try:
        # attempt to normalize common confusables into ASCII names; keep simple
        l.encode("ascii", "ignore")
    except Exception:
        pass
    return f"{l}@{d}"

# ----------------------------
# Risk → confidence heuristic
# ----------------------------

def _risk_score(risks: List[str]) -> float:
    return sum(RISK_WEIGHTS.get(r, 0.2) for r in set(risks))

def _confidence_for(action: str, risks: List[str], fix_kind: Optional[str]) -> float:
    base = 0.98 if action == "accept" and not risks else 0.9 if action == "fix_auto" else 0.4 if action == "review" else 0.1
    penalty = min(0.6, _risk_score(risks) * 0.15)
    bonus = 0.05 if (action == "fix_auto" and fix_kind == "exact") else 0.0
    return max(0.01, min(0.99, base - penalty + bonus))

# ----------------------------
# Main entrypoint
# ----------------------------

def validate_email_deterministic(
    email: str,
    *,
    exclude_role_accounts: bool = False,
    disposable_set: Optional[set] = None,
    role_locals: Optional[set] = None,
    top_domains: Optional[List[str]] = None,
    tld_whitelist: Optional[set] = None,
) -> DeterministicResult:
    """
    Validate a single email deterministically, propose safe fix if obvious, and decide:
    - accept: looks fine (after normalization)
    - fix_auto: one unambiguous correction is advisable
    - review: edge case; leave as-is for human/LLM
    - suppress: clearly junk or invalid (remove from dataset)

    Returns DeterministicResult with canonical_key for dedupe.
    """
    original = email or ""
    normalized, norm_flags = normalize_email_raw(original)
    risks: List[str] = []
    notes_parts: List[str] = []

    # Basic structure checks
    if normalized.count("@") != 1:
        risks += ["invalid_syntax", "multiple_at"] if normalized.count("@") > 1 else ["invalid_syntax"]
        action = "suppress"
        conf = _confidence_for(action, risks, None)
        return DeterministicResult(original, normalized, action, conf, sorted(set(risks+norm_flags)), None,
                                   "Invalid '@' count", None)

    local, domain = split_local_domain(normalized)

    # IDNA conversion for domain
    domain_ascii, idn_flags = idna_ascii(domain)
    risks += idn_flags

    # Domain structure & TLD sanity
    risks += validate_domain_structure(domain_ascii)
    if not is_known_tld(domain_ascii, tld_whitelist):
        risks.append("bad_tld")

    # Local-part syntax
    risks += validate_local_part(local)

    # Flags: role, freemail, disposable
    if classify_role_account(local, role_locals):
        risks.append("role_account")
    if classify_free_mail(domain_ascii):
        risks.append("free_mail_domain")
    if classify_disposable(domain_ascii, disposable_set):
        risks.append("disposable_domain")

    # Suggest safe, single-step fixes
    suggested_fix = None
    fix_kind = None

    # 1) TLD one-step fix
    tld_fix = suggest_tld_fix(domain_ascii)
    if tld_fix:
        suggested_fix = f"{local}@{tld_fix}"
        fix_kind = "exact"

    # 2) Exact domain typo fix (overrides tld fix)
    exact = suggest_exact_domain_fix(domain_ascii)
    if exact:
        suggested_fix = f"{local}@{exact}"
        fix_kind = "exact"

    # 3) Local double-dot → single dot (only if other syntax ok)
    if not suggested_fix and "double_dot_local" in risks:
        lfix = collapse_local_double_dot(local)
        if lfix and not lfix.startswith(".") and not lfix.endswith("."):
            suggested_fix = f"{lfix}@{domain_ascii}"
            fix_kind = "local_collapse"

    # 4) Fuzzy domain fix (only if domain label invalid OR bad_tld)
    if not suggested_fix and (("invalid_domain_label" in risks) or ("bad_tld" in risks)):
        fz = suggest_fuzzy_domain_fix(domain_ascii, top_domains)
        if fz:
            suggested_fix = f"{local}@{fz}"
            fix_kind = "fuzzy_domain"

    # Decide action
    # Hard suppress if fundamentally broken or disposable
    fundamental = {"invalid_syntax", "multiple_at", "empty_local", "empty_domain", "invalid_domain_label"}
    risky_any = any(r in risks for r in fundamental) or ("bad_tld" in risks) or ("disposable_domain" in risks)

    if exclude_role_accounts and "role_account" in risks:
        action = "suppress"
        notes_parts.append("Role account excluded by policy")
    elif risky_any and not suggested_fix:
        action = "suppress"
        notes_parts.append("Fundamental or high-risk issue with no safe fix")
    elif suggested_fix:
        # If only mild risks remain after a safe fix, prefer fix_auto
        action = "fix_auto"
        notes_parts.append(f"Suggested fix ({fix_kind})")
    else:
        # Accept but keep minor risk flags for transparency
        action = "accept"

    # Confidence heuristic
    conf = _confidence_for(action, risks, fix_kind)

    # If many medium risks and no fix, downgrade to review (don't overwrite)
    medium_risks = {"unicode_confusable", "non_ascii_domain", "role_account"}
    if action == "accept" and len(set(risks) & medium_risks) >= 2:
        action = "review"
        notes_parts.append("Multiple medium risks; review recommended")
        conf = _confidence_for(action, risks, None)

    # Build final normalized email
    final_email = f"{local}@{domain_ascii}".lower() if classify_free_mail(domain_ascii) else f"{local}@{domain_ascii}"
    
    # Build canonical key for dedupe using the output email (fixed if available)
    key_email = suggested_fix if suggested_fix else final_email
    can_key = None if action == "suppress" else canonical_key(key_email, provider_aware=True)

    result = DeterministicResult(
        input_email=original,
        normalized_email=final_email,
        action=action,
        confidence=round(conf, 3),
        risk_reasons=sorted(set(risks + norm_flags)),
        suggested_fix=suggested_fix,
        notes="; ".join(notes_parts)[:160],
        canonical_key=can_key
    )
    return result

# ----------------------------
# Batch helpers
# ----------------------------

def dedupe_by_canonical(emails: List[str], provider_aware: bool = True) -> Tuple[Dict[str, int], Dict[str, List[int]]]:
    """
    Returns (keeper_index_by_key, duplicates_by_key).
    - keeper_index_by_key[key] = index of the email to keep
    - duplicates_by_key[key] = indices of other emails to drop
    """
    keeper: Dict[str, int] = {}
    dups: Dict[str, List[int]] = {}
    for idx, em in enumerate(emails):
        key = canonical_key(em, provider_aware=provider_aware)
        if key is None:
            continue
        if key not in keeper:
            keeper[key] = idx
        else:
            dups.setdefault(key, []).append(idx)
    return keeper, dups

def load_reference_sets() -> Dict[str, object]:
    """
    Load configurable lists if present; fall back to small embedded defaults.
    """
    disposable = set(_load_list(os.path.join("config", "disposable_domains.txt")))
    roles = set(_load_list(os.path.join("config", "role_locals.txt"))) or EMBED_ROLE_LOCAL_PARTS
    top_domains = _load_list(os.path.join("config", "top_domains.txt"))
    # Optional: tld whitelist from file
    tld_whitelist = set(_load_list(os.path.join("config", "tlds.txt"))) or None
    return {
        "disposable_set": disposable,
        "role_locals": roles,
        "top_domains": top_domains,
        "tld_whitelist": tld_whitelist,
    }

# ----------------------------
# Example (remove or adapt in production)
# ----------------------------
if __name__ == "__main__":
    refs = load_reference_sets()
    samples = [
        " anna@company.con ",
        "john..doe@example.com",
        "sales@acme.com",
        "sarah+events@gmail.com",
        "iván@exámple.com",
        "mark@faceboook.com",
        "test@test.com",
        "nope@@domain.com",
        "user@disposablemail.com"
    ]
    for s in samples:
        res = validate_email_deterministic(s, exclude_role_accounts=True, **refs)
        print(json.dumps(res.as_dict(), ensure_ascii=False))
