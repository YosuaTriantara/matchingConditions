import requests
import pandas as pd
from supabase import create_client
from typing import Tuple
import uuid
import math

# Konfigurasi Supabase
SUPABASE_URL = "https://uiglmxbrvdmgcfqrfsgc.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVpZ2xteGJydmRtZ2NmcXJmc2djIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTc3MjM4ODMsImV4cCI6MjA3MzI5OTg4M30.ZJiGqyrT8CF1EMBx6yKW_xzR4t6SzYH6OHTxqCgZ0p8"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def safe_uuid(value):
    """Return UUID if valid, else None"""
    if not value:
        return None
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError):
        return None

def safe_float(value):
    """Convert value to float, handling NaN and None"""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def fetch_api(url: str):
    """Fetch data dari API (return list of dict)."""
    if not url:
        return []
    resp = requests.get(url)
    resp.raise_for_status()
    payload = resp.json()
    return payload.get("data", []) if isinstance(payload, dict) else payload

def save_users_from_records(records):
    users = []
    for r in records:
        user = r.get("user")
        if user and user.get("id"):
            users.append({
                "id": user["id"],
                "name": user.get("name"),
                "username": user.get("username"),
                "avatar_url": user.get("avatar_url"),
            })

    # deduplicate by id
    unique_users = {}
    for u in users:
        uid = u["id"]
        if uid not in unique_users:
            unique_users[uid] = u
        else:
            unique_users[uid].update(u)

    final_users = list(unique_users.values())

    if final_users:
        for u in final_users:
            supabase.table("users").upsert(u, on_conflict=["id"]).execute()

def save_ideas_normalized(ideas):
    """Save ideas tanpa challenge_id - clean entity"""
    if not ideas:
        return []
    
    normalized = []
    for record in ideas:
        idea = {
            "id": safe_uuid(record.get("id")),
            "title": record.get("title"),
            "description": record.get("description"),
            "tags": record.get("tags", []),
            "creator_id": safe_uuid(record.get("user", {}).get("id")),
            "votes": record.get("votes", 0),
            "comments": record.get("comments", 0),
            "created_at": record.get("createdAt"),
        }
        
        if not idea["id"] or not idea["creator_id"]:
            continue
            
        normalized.append(idea)
    
    if normalized:
        supabase.table("ideas").upsert(normalized, on_conflict=["id"]).execute()
    
    return normalized

def save_campaigns_normalized(campaigns):
    """Save campaigns tanpa idea_id/challenge_id - clean entity"""
    if not campaigns:
        return []
        
    normalized = []
    for record in campaigns:
        campaign = {
            "id": safe_uuid(record.get("id")),
            "title": record.get("title"),
            "description": record.get("description"),
            "category": record.get("category"),
            "trigger_type": record.get("triggerType"),
            "trigger_count": record.get("triggerCount"),
            "trigger_clause": record.get("triggerClause"),
            "trigger_amount": record.get("triggerAmount"),
            "preorder_price": record.get("preorderPrice"),
            "deadline": record.get("deadline"),
            "perks": record.get("perks"),
            "referral_reward": record.get("referralReward"),
            "custom_questions": record.get("customQuestions"),
            "banner_image": record.get("bannerImage"),
            "creator_id": safe_uuid(record.get("user", {}).get("id")),
            "supports": record.get("supports", 0),
            "votes": record.get("votes", 0),
            "comments": record.get("comments", 0),
            "created_at": record.get("createdAt"),
        }
        
        if not campaign["id"] or not campaign["creator_id"]:
            continue
            
        normalized.append(campaign)
    
    if normalized:
        supabase.table("campaigns").upsert(normalized, on_conflict=["id"]).execute()
    
    return normalized

def save_challenges_normalized_without_conditions_column(challenges):
    """Save challenges TANPA kolom conditions (karena belum ada di DB)"""
    if not challenges:
        return []
        
    normalized = []
    conditions_to_save = []
    
    # Debug: check berapa challenges yang punya conditions
    challenges_with_conditions = 0
    total_conditions = 0
    
    for record in challenges:
        conditions = record.get("conditions", [])
        if conditions:
            challenges_with_conditions += 1
            total_conditions += len(conditions) if isinstance(conditions, list) else 1
        
        # Save challenge WITHOUT conditions field (karena kolom tidak ada)
        challenge = {
            "id": safe_uuid(record.get("id")),
            "title": record.get("title"),
            "description": record.get("description"),
            "tags": record.get("tags", []),
            "type": record.get("type"),
            "image": record.get("image"),
            "deadline": record.get("deadline"),
            "rewards": record.get("rewards", []),
            "creator_id": safe_uuid(record.get("user", {}).get("id")),
            "created_at": record.get("createdAt"),
            "updated_at": record.get("updatedAt"),
            # TIDAK ada conditions field karena kolom belum ada di DB
        }
        
        if not challenge["id"] or not challenge["creator_id"]:
            continue

        # Save conditions to separate table
        if conditions:
            if isinstance(conditions, list):
                for cond in conditions:
                    conditions_to_save.append({
                        "challenge_id": challenge['id'],
                        "kind": cond.get("kind"),
                        "field": cond.get("field"),
                        "value": cond.get("value"),
                        "operator": cond.get("operator"),
                        "words": cond.get("words"),
                    })
            else:
                # Single condition (not in array)
                conditions_to_save.append({
                    "challenge_id": challenge['id'],
                    "kind": conditions.get("kind"),
                    "field": conditions.get("field"),
                    "value": conditions.get("value"),
                    "operator": conditions.get("operator"),
                    "words": conditions.get("words"),
                })
            
        normalized.append(challenge)
    
    print(f"📊 Challenges processing:")
    print(f"   Total challenges: {len(normalized)}")
    print(f"   Challenges with conditions: {challenges_with_conditions}")
    print(f"   Total conditions: {total_conditions}")
    
    # Save challenges (tanpa conditions field)
    if normalized:
        supabase.table("challenges").upsert(normalized, on_conflict=["id"]).execute()
        print(f"✅ Saved {len(normalized)} challenges to main table")
    
    # Save conditions to separate table
    if conditions_to_save:
        supabase.table("challenge_conditions").upsert(conditions_to_save).execute()
        print(f"✅ Saved {len(conditions_to_save)} conditions to separate table")
    
    return normalized

def load_conditions_from_separate_table(challenge_ids: list) -> dict:
    """
    Load conditions dari tabel challenge_conditions dan return sebagai dict
    """
    if not challenge_ids:
        return {}
    
    try:
        result = supabase.table("challenge_conditions").select("*").in_("challenge_id", challenge_ids).execute()
        
        conditions_by_challenge = {}
        for condition in result.data:
            challenge_id = condition["challenge_id"]
            if challenge_id not in conditions_by_challenge:
                conditions_by_challenge[challenge_id] = []
            
            # Create condition dict
            cond_dict = {
                "kind": condition.get("kind"),
                "field": condition.get("field"),
                "value": condition.get("value"),
                "operator": condition.get("operator"),
                "words": condition.get("words", [])
            }
            
            # Check for duplicates before adding
            is_duplicate = False
            for existing_cond in conditions_by_challenge[challenge_id]:
                if (existing_cond.get("kind") == cond_dict.get("kind") and
                    existing_cond.get("field") == cond_dict.get("field") and
                    existing_cond.get("operator") == cond_dict.get("operator") and
                    str(existing_cond.get("value")) == str(cond_dict.get("value")) and
                    str(existing_cond.get("words")) == str(cond_dict.get("words"))):
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                conditions_by_challenge[challenge_id].append(cond_dict)
        
        # Debug info
        for challenge_id, conditions in conditions_by_challenge.items():
            if len(conditions) > 5:
                print(f"⚠️ Challenge {challenge_id} has {len(conditions)} conditions (might be duplicates)")
        
        print(f"✅ Loaded conditions for {len(conditions_by_challenge)} challenges from separate table")
        return conditions_by_challenge
        
    except Exception as e:
        print(f"⚠️ Error loading conditions from separate table: {e}")
        return {}

def load_data(ideas_url, campaigns_url, challenges_url):
    """Backward compatibility function"""
    return load_and_save_normalized(ideas_url, campaigns_url, challenges_url)

def load_and_save_normalized(ideas_url, campaigns_url, challenges_url):
    """
    Main function yang menghandle conditions dari separate table
    """
    print("🔥 Fetching data from APIs...")
    
    # 1. Fetch raw data
    ideas_raw = fetch_api(ideas_url)
    campaigns_raw = fetch_api(campaigns_url)
    challenges_raw = fetch_api(challenges_url)
    
    print(f"📊 Fetched: {len(ideas_raw)} ideas, {len(campaigns_raw)} campaigns, {len(challenges_raw)} challenges")
    
    # 2. Save users first
    save_users_from_records(ideas_raw + campaigns_raw + challenges_raw)
    
    # 3. Save core entities
    ideas_saved = save_ideas_normalized(ideas_raw)
    campaigns_saved = save_campaigns_normalized(campaigns_raw)
    challenges_saved = save_challenges_normalized_without_conditions_column(challenges_raw)
    
    # 4. Convert to DataFrames
    ideas_df = pd.DataFrame(ideas_saved) if ideas_saved else pd.DataFrame()
    campaigns_df = pd.DataFrame(campaigns_saved) if campaigns_saved else pd.DataFrame()
    challenges_df = pd.DataFrame(challenges_saved) if challenges_saved else pd.DataFrame()
    
    # 5. PENTING: Load conditions dan merge ke DataFrame
    if not challenges_df.empty:
        challenge_ids = challenges_df['id'].tolist()
        conditions_dict = load_conditions_from_separate_table(challenge_ids)
        
        # Add conditions column to DataFrame
        def get_conditions_for_challenge(challenge_id):
            return conditions_dict.get(challenge_id, [])
        
        challenges_df['conditions'] = challenges_df['id'].apply(get_conditions_for_challenge)
        
        # Debug: Check berapa challenges yang dapat conditions
        challenges_with_conditions = sum(1 for conditions in challenges_df['conditions'] if conditions)
        print(f"📊 Final DataFrame: {challenges_with_conditions}/{len(challenges_df)} challenges have conditions")
    
    return ideas_df, campaigns_df, challenges_df

# Updated functions with correct table names
def save_idea_recommendation(
    challenge_id: str,
    idea_id: str,
    rule_score: float = 0.0,
    similarity_score: float = 0.0,
    final_score: float = 0.0,
    rank: int = None
) -> bool:
    """
    Save idea recommendation to challenge_idea_recommendations table
    """
    if not challenge_id or not idea_id:
        print(f"❌ Missing IDs: challenge_id={challenge_id}, idea_id={idea_id}")
        return False

    record = {
        "challenge_id": challenge_id,
        "idea_id": idea_id,
        "rule_score": safe_float(rule_score),
        "similarity_score": safe_float(similarity_score),
        "final_score": safe_float(final_score),
        "rank": rank,
        "created_at": pd.Timestamp.utcnow().isoformat()
    }

    try:
        supabase.table("challenge_idea_recommendations").upsert(
            record,
            on_conflict=["challenge_id", "idea_id"]
        ).execute()
        print(f"✅ Saved idea recommendation: {challenge_id} -> {idea_id}")
        return True
    except Exception as e:
        print(f"❌ Error saving idea recommendation: {e}")
        return False

def save_campaign_recommendation(
    challenge_id: str,
    campaign_id: str,
    rule_score: float = 0.0,
    similarity_score: float = 0.0,
    final_score: float = 0.0,
    rank: int = None
) -> bool:
    """
    Save campaign recommendation to challenge_campaign_recommendations table
    """
    if not challenge_id or not campaign_id:
        print(f"❌ Missing IDs: challenge_id={challenge_id}, campaign_id={campaign_id}")
        return False

    record = {
        "challenge_id": challenge_id,
        "campaign_id": campaign_id,
        "rule_score": safe_float(rule_score),
        "similarity_score": safe_float(similarity_score),
        "final_score": safe_float(final_score),
        "rank": rank,
        "created_at": pd.Timestamp.utcnow().isoformat()
    }

    try:
        supabase.table("challenge_campaign_recommendations").upsert(
            record,
            on_conflict=["challenge_id", "campaign_id"]
        ).execute()
        print(f"✅ Saved campaign recommendation: {challenge_id} -> {campaign_id}")
        return True
    except Exception as e:
        print(f"❌ Error saving campaign recommendation: {e}")
        return False

# Legacy functions for backward compatibility (deprecated)
def save_challenge_idea(challenge_id: str, idea_id: str, rule_score: float = 0.0, 
                       sim_score: float = 0.0, final_score: float = 0.0):
    """Deprecated: Use save_idea_recommendation instead"""
    print("⚠️ save_challenge_idea is deprecated, use save_idea_recommendation")
    return save_idea_recommendation(challenge_id, idea_id, rule_score, sim_score, final_score)

def save_challenge_campaign(challenge_id: str, campaign_id: str, rule_score: float = 0.0, 
                           sim_score: float = 0.0, final_score: float = 0.0):
    """Deprecated: Use save_campaign_recommendation instead"""
    print("⚠️ save_challenge_campaign is deprecated, use save_campaign_recommendation")
    return save_campaign_recommendation(challenge_id, campaign_id, rule_score, sim_score, final_score)