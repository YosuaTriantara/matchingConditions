import pandas as pd

def filter_candidates_by_type(challenge: dict, ideas: pd.DataFrame, campaigns: pd.DataFrame) -> pd.DataFrame:
    t = (challenge.get("type") or "both").lower()
    if t == "idea" or t == "ideas":
        return ideas.reset_index(drop=True)
    elif t == "campaign" or t == "campaigns" or t == "campaigns":
        return campaigns.reset_index(drop=True)
    else:
        return pd.concat([ideas, campaigns], ignore_index=True).reset_index(drop=True)
