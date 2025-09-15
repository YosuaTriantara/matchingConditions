import json
import re
from typing import Any
import pandas as pd


# minimal english stopwords for demo; replace with nltk or sklearn stopwords if needed
STOPWORDS = set(["the", "a", "an", "and", "or", "is", "it", "to", "for", "of"])

def parse_tags_field(v: Any):
    """Normalize tags field: some records keep tags as a JSON-string like '["tag"]' while others
    already as list. Return list[str]."""
    if v is None:
        return []
    if isinstance(v, list):
        return v
    if isinstance(v, str):
        # try parse as json list
        s = v.strip()
        try:
            if s.startswith("["):
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return parsed
        except Exception:
            # fall through to simple split
            pass
        # fallback: split by non-alphanumeric
        tokens = re.findall(r"[a-zA-Z0-9_\-]+", s)
        return tokens
    return []

def clean_text(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    s = text.lower()
    s = re.sub(r"http\S+", "", s)
    s = re.sub(r"[^a-z0-9\s#@]+", " ", s)
    tokens = [t for t in s.split() if t and t not in STOPWORDS]
    return " ".join(tokens)


def preprocess_dataframe(df: pd.DataFrame, text_cols=("title", "description")) -> pd.DataFrame:
    df = df.copy()
    # ensure tags normalized
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(parse_tags_field)
    else:
        df["tags"] = [[] for _ in range(len(df))]
    # normalize text columns
    return df