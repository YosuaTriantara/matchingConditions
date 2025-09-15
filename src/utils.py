def normalize_tags(tags):
    """
    Normalize tags field to always return list of strings.
    - If string like '["tag1","tag2"]', convert to ["tag1","tag2"]
    - If plain string, wrap into list
    - If already list, return as-is
    """
    if tags is None:
        return []

    if isinstance(tags, str):
        # hapus bracket JSON-like
        tags = tags.strip("[]").replace('"', "").replace("'", "")
        return [t.strip() for t in tags.split(",") if t.strip()]

    if isinstance(tags, list):
        return [str(t).strip() for t in tags if str(t).strip()]

    return []

