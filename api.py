from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import uuid
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import os
import sys
import time

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.getData import (
    load_and_save_normalized, 
    save_idea_recommendation, 
    save_campaign_recommendation,
    supabase
)
from src.preprocessing import preprocess_dataframe
from src.matching import filter_candidates_by_type
from src.ruledBased import rule_based_match_improved
from src.similarity import build_similarity_engine
from src.ranking import combine_scores_improved
from src.utils import normalize_tags

app = Flask(__name__)
CORS(app)

load_dotenv()
VALID_API_KEY = os.getenv("ML_API_KEY")

# Global URLs
IDEAS_URL = "https://favbackend-dev.vercel.app/api/yos/ideas/list"
CAMPAIGNS_URL = "https://favbackend-dev.vercel.app/api/yos/campaigns/list"
CHALLENGES_URL = "https://favbackend-dev.vercel.app/api/yos/challenges/list"

def check_api_key():
    # Skip untuk static files atau endpoint tertentu
    if request.endpoint in ["static", "health_check"]:
        return

    # Ambil API Key dari header
    api_key = request.headers.get("Authorization")

    if not api_key:
        return jsonify({"error": "API Key is missing"}), 401
    if api_key != VALID_API_KEY:
        return jsonify({"error": "Invalid API Key"}), 403
    
def is_valid_uuid(uuid_string: str) -> bool:
    """Check if string is valid UUID"""
    try:
        uuid.UUID(uuid_string)
        return True
    except (ValueError, TypeError):
        return False

#@app.before_request
#def require_api_key():
    print("Headers received:", dict(request.headers)) 
    result = check_api_key()
    if result:  # jika check_api_key mengembalikan error response
        return result
    return None

@app.route("/health", methods=["GET"])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "recommendation-engine"
    })

@app.route("/matches/campaigns", methods=["POST"])
def generate_campaign_matches():
    """
    Generate campaign matches in the requested format:
    {
      "matches": [
        {
          "challengeId": "challenge_123",
          "campaignIds": ["campaign_456", "campaign_789"],
          "similarityScore": [0.85, 0.72],
          "ruleScore": [0.50, 1.00],
          "finalScore": [0.80, 0.90]
        }
      ],
      "processingTime": "150ms"
    }
    """
    start_time = time.time()
    
    try:
        body = request.json or {}
        ideas_url = body.get("ideas_url", IDEAS_URL)
        campaigns_url = body.get("campaigns_url", CAMPAIGNS_URL)
        challenges_url = body.get("challenges_url", CHALLENGES_URL)
        
        # Parameters
        challenge_id = body.get("challenge_id")
        save_to_db = body.get("save_to_db", True)
        min_score = body.get("min_score", 0.1)
        limit = body.get("limit", 10)
        
        # Load and preprocess data
        ideas, campaigns, challenges = load_and_save_normalized(ideas_url, campaigns_url, challenges_url)
        ideas = preprocess_dataframe(ideas)
        campaigns = preprocess_dataframe(campaigns)
        challenges = preprocess_dataframe(challenges)
        
        # Filter specific challenge if requested
        if challenge_id:
            if not is_valid_uuid(challenge_id):
                return jsonify({"error": "Invalid challenge_id format"}), 400
            challenges = challenges[challenges['id'] == challenge_id]
            if challenges.empty:
                return jsonify({"error": "Challenge not found"}), 404

        # Build similarity engine
        engine = build_similarity_engine(pd.concat([ideas, campaigns]), challenges)
        
        matches = []
        
        for _, ch_row in challenges.iterrows():
            challenge = ch_row.to_dict()
            cid = challenge.get("id")
            
            candidates = filter_candidates_by_type(challenge, ideas, campaigns)
            if candidates.empty:
                continue
                
            matched = rule_based_match_improved(
                challenge, candidates, min_conditions_passed=1, min_score_threshold=min_score
            )
            if not matched:
                continue

            challenge_text = " ".join([
                str(challenge.get("title", "")),
                str(challenge.get("description", "")),
                " ".join(normalize_tags(challenge.get("tags", [])))
            ])
            
            # Filter only campaigns
            campaign_recs = []
            
            for r in matched:
                try:
                    # Check if it's a campaign
                    is_campaign = any(field in r["raw"] for field in 
                                    ["trigger_type", "preorder_price", "supports"])
                    
                    if not is_campaign:
                        continue
                    
                    candidate_text = " ".join([
                        str(r["raw"].get("title", "")),
                        str(r["raw"].get("description", "")),
                        " ".join(normalize_tags(r["raw"].get("tags", [])))
                    ])
                    
                    sim_score = engine.compute(challenge_text, candidate_text)
                    engagement = (r["raw"].get("votes", 0) or 0) + (r["raw"].get("supports", 0) or 0)
                    final_score = combine_scores_improved(r["score"], sim_score, engagement)
                    
                    campaign_recs.append({
                        "id": r["id"],
                        "rule_score": r["score"],
                        "similarity_score": sim_score,
                        "final_score": final_score
                    })
                    
                    # Save to database if requested
                    if save_to_db:
                        save_campaign_recommendation(cid, r["id"], r["score"], sim_score, final_score)
                        
                except Exception as e:
                    continue
            
            # Sort and limit
            campaign_recs.sort(key=lambda x: x["final_score"], reverse=True)
            limited_campaigns = campaign_recs[:limit]
            
            # Format in requested structure
            if limited_campaigns:
                match = {
                    "challengeId": cid,
                    "campaignIds": [r["id"] for r in limited_campaigns],
                    "similarityScore": [round(r["similarity_score"], 3) for r in limited_campaigns],
                    "ruleScore": [round(r["rule_score"], 3) for r in limited_campaigns],
                    "finalScore": [round(r["final_score"], 3) for r in limited_campaigns]
                }
                matches.append(match)
        
        processing_time = f"{int((time.time() - start_time) * 1000)}ms"
        
        return jsonify({
            "matches": matches,
            "processingTime": processing_time
        })
        
    except Exception as e:
        processing_time = f"{int((time.time() - start_time) * 1000)}ms"
        return jsonify({
            "error": str(e),
            "processingTime": processing_time
        }), 500

@app.route("/matches/ideas", methods=["POST"])
def generate_idea_matches():
    """
    Generate idea matches in the requested format:
    {
      "matches": [
        {
          "challengeId": "challenge_123",
          "ideaIds": ["idea_456", "idea_789"],
          "similarityScore": [0.85, 0.72],
          "ruleScore": [0.50, 1.00],
          "finalScore": [0.80, 0.90]
        }
      ],
      "processingTime": "150ms"
    }
    """
    start_time = time.time()
    
    try:
        body = request.json or {}
        ideas_url = body.get("ideas_url", IDEAS_URL)
        campaigns_url = body.get("campaigns_url", CAMPAIGNS_URL)
        challenges_url = body.get("challenges_url", CHALLENGES_URL)
        
        # Parameters
        challenge_id = body.get("challenge_id")
        save_to_db = body.get("save_to_db", True)
        min_score = body.get("min_score", 0.1)
        limit = body.get("limit", 10)
        
        # Load and preprocess data
        ideas, campaigns, challenges = load_and_save_normalized(ideas_url, campaigns_url, challenges_url)
        ideas = preprocess_dataframe(ideas)
        campaigns = preprocess_dataframe(campaigns)
        challenges = preprocess_dataframe(challenges)
        
        # Filter specific challenge if requested
        if challenge_id:
            if not is_valid_uuid(challenge_id):
                return jsonify({"error": "Invalid challenge_id format"}), 400
            challenges = challenges[challenges['id'] == challenge_id]
            if challenges.empty:
                return jsonify({"error": "Challenge not found"}), 404

        # Build similarity engine
        engine = build_similarity_engine(pd.concat([ideas, campaigns]), challenges)
        
        matches = []
        
        for _, ch_row in challenges.iterrows():
            challenge = ch_row.to_dict()
            cid = challenge.get("id")
            
            candidates = filter_candidates_by_type(challenge, ideas, campaigns)
            if candidates.empty:
                continue
                
            matched = rule_based_match_improved(
                challenge, candidates, min_conditions_passed=1, min_score_threshold=min_score
            )
            if not matched:
                continue

            challenge_text = " ".join([
                str(challenge.get("title", "")),
                str(challenge.get("description", "")),
                " ".join(normalize_tags(challenge.get("tags", [])))
            ])
            
            # Filter only ideas
            idea_recs = []
            
            for r in matched:
                try:
                    # Check if it's an idea (not a campaign)
                    is_campaign = any(field in r["raw"] for field in 
                                    ["trigger_type", "preorder_price", "supports"])
                    
                    if is_campaign:
                        continue
                    
                    candidate_text = " ".join([
                        str(r["raw"].get("title", "")),
                        str(r["raw"].get("description", "")),
                        " ".join(normalize_tags(r["raw"].get("tags", [])))
                    ])
                    
                    sim_score = engine.compute(challenge_text, candidate_text)
                    engagement = (r["raw"].get("votes", 0) or 0) + (r["raw"].get("comments", 0) or 0)
                    final_score = combine_scores_improved(r["score"], sim_score, engagement)
                    
                    idea_recs.append({
                        "id": r["id"],
                        "rule_score": r["score"],
                        "similarity_score": sim_score,
                        "final_score": final_score
                    })
                    
                    # Save to database if requested
                    if save_to_db:
                        save_idea_recommendation(cid, r["id"], r["score"], sim_score, final_score)
                        
                except Exception as e:
                    continue
            
            # Sort and limit
            idea_recs.sort(key=lambda x: x["final_score"], reverse=True)
            limited_ideas = idea_recs[:limit]
            
            # Format in requested structure
            if limited_ideas:
                match = {
                    "challengeId": cid,
                    "ideaIds": [r["id"] for r in limited_ideas],
                    "similarityScore": [round(r["similarity_score"], 3) for r in limited_ideas],
                    "ruleScore": [round(r["rule_score"], 3) for r in limited_ideas],
                    "finalScore": [round(r["final_score"], 3) for r in limited_ideas]
                }
                matches.append(match)
        
        processing_time = f"{int((time.time() - start_time) * 1000)}ms"
        
        return jsonify({
            "matches": matches,
            "processingTime": processing_time
        })
        
    except Exception as e:
        processing_time = f"{int((time.time() - start_time) * 1000)}ms"
        return jsonify({
            "error": str(e),
            "processingTime": processing_time
        }), 500

@app.route("/matches", methods=["POST"])
def generate_all_matches():
    """
    Generate both campaign and idea matches in single pass:
    {
      "campaign_matches": [...],
      "idea_matches": [...],
      "processingTime": "150ms"
    }
    """
    start_time = time.time()
    
    try:
        body = request.json or {}
        ideas_url = body.get("ideas_url", IDEAS_URL)
        campaigns_url = body.get("campaigns_url", CAMPAIGNS_URL)
        challenges_url = body.get("challenges_url", CHALLENGES_URL)
        
        # Parameters
        challenge_id = body.get("challenge_id")
        save_to_db = body.get("save_to_db", True)
        min_score = body.get("min_score", 0.1)
        limit = body.get("limit", 10)
        
        # Load and preprocess data (single pass)
        ideas, campaigns, challenges = load_and_save_normalized(ideas_url, campaigns_url, challenges_url)
        ideas = preprocess_dataframe(ideas)
        campaigns = preprocess_dataframe(campaigns)
        challenges = preprocess_dataframe(challenges)
        
        # Filter specific challenge if requested
        if challenge_id:
            if not is_valid_uuid(challenge_id):
                return jsonify({"error": "Invalid challenge_id format"}), 400
            challenges = challenges[challenges['id'] == challenge_id]
            if challenges.empty:
                return jsonify({"error": "Challenge not found"}), 404

        # Build similarity engine (single pass)
        engine = build_similarity_engine(pd.concat([ideas, campaigns]), challenges)
        
        campaign_matches = []
        idea_matches = []
        
        for _, ch_row in challenges.iterrows():
            challenge = ch_row.to_dict()
            cid = challenge.get("id")
            
            candidates = filter_candidates_by_type(challenge, ideas, campaigns)
            if candidates.empty:
                continue
                
            matched = rule_based_match_improved(
                challenge, candidates, min_conditions_passed=1, min_score_threshold=min_score
            )
            if not matched:
                continue

            challenge_text = " ".join([
                str(challenge.get("title", "")),
                str(challenge.get("description", "")),
                " ".join(normalize_tags(challenge.get("tags", [])))
            ])
            
            campaign_recs = []
            idea_recs = []
            
            for r in matched:
                try:
                    candidate_text = " ".join([
                        str(r["raw"].get("title", "")),
                        str(r["raw"].get("description", "")),
                        " ".join(normalize_tags(r["raw"].get("tags", [])))
                    ])
                    
                    sim_score = engine.compute(challenge_text, candidate_text)
                    engagement = (r["raw"].get("votes", 0) or 0) + (r["raw"].get("supports", 0) or 0) + (r["raw"].get("comments", 0) or 0)
                    final_score = combine_scores_improved(r["score"], sim_score, engagement)
                    
                    rec_data = {
                        "id": r["id"],
                        "rule_score": r["score"],
                        "similarity_score": sim_score,
                        "final_score": final_score
                    }
                    
                    # Determine type
                    is_campaign = any(field in r["raw"] for field in 
                                    ["trigger_type", "preorder_price", "supports"])
                    
                    if is_campaign:
                        campaign_recs.append(rec_data)
                        if save_to_db:
                            save_campaign_recommendation(cid, r["id"], r["score"], sim_score, final_score)
                    else:
                        idea_recs.append(rec_data)
                        if save_to_db:
                            save_idea_recommendation(cid, r["id"], r["score"], sim_score, final_score)
                        
                except Exception as e:
                    continue
            
            # Sort and limit campaigns
            campaign_recs.sort(key=lambda x: x["final_score"], reverse=True)
            limited_campaigns = campaign_recs[:limit]
            
            if limited_campaigns:
                campaign_match = {
                    "challengeId": cid,
                    "campaignIds": [r["id"] for r in limited_campaigns],
                    "similarityScore": [round(r["similarity_score"], 3) for r in limited_campaigns],
                    "ruleScore": [round(r["rule_score"], 3) for r in limited_campaigns],
                    "finalScore": [round(r["final_score"], 3) for r in limited_campaigns]
                }
                campaign_matches.append(campaign_match)
            
            # Sort and limit ideas
            idea_recs.sort(key=lambda x: x["final_score"], reverse=True)
            limited_ideas = idea_recs[:limit]
            
            if limited_ideas:
                idea_match = {
                    "challengeId": cid,
                    "ideaIds": [r["id"] for r in limited_ideas],
                    "similarityScore": [round(r["similarity_score"], 3) for r in limited_ideas],
                    "ruleScore": [round(r["rule_score"], 3) for r in limited_ideas],
                    "finalScore": [round(r["final_score"], 3) for r in limited_ideas]
                }
                idea_matches.append(idea_match)
        
        processing_time = f"{int((time.time() - start_time) * 1000)}ms"
        
        return jsonify({
            "campaign_matches": campaign_matches,
            "idea_matches": idea_matches,
            "processingTime": processing_time
        })
        
    except Exception as e:
        processing_time = f"{int((time.time() - start_time) * 1000)}ms"
        return jsonify({
            "error": str(e),
            "processingTime": processing_time
        }), 500

# Keep existing endpoints for backward compatibility
@app.route("/recommendations/<challenge_id>", methods=["GET"])
def get_recommendations(challenge_id: str):
    """Get saved recommendations for a specific challenge"""
    try:
        if not is_valid_uuid(challenge_id):
            return jsonify({"error": "Invalid challenge_id format"}), 400
        
        limit = request.args.get("limit", 10, type=int)
        include_raw = request.args.get("include_raw", "false").lower() == "true"
        
        # Get idea recommendations
        idea_recs = supabase.table("challenge_idea_recommendations")\
            .select("*, ideas!inner(*)")\
            .eq("challenge_id", challenge_id)\
            .order("rank", desc=False)\
            .order("final_score", desc=True)\
            .limit(limit)\
            .execute()
        
        # Get campaign recommendations  
        campaign_recs = supabase.table("challenge_campaign_recommendations")\
            .select("*, campaigns!inner(*)")\
            .eq("challenge_id", challenge_id)\
            .order("rank", desc=False)\
            .order("final_score", desc=True)\
            .limit(limit)\
            .execute()
        
        # Format response
        recommendations = []
        
        # Process idea recommendations
        for rec in idea_recs.data:
            item = {
                "id": rec["ideas"]["id"],
                "title": rec["ideas"]["title"],
                "description": rec["ideas"]["description"],
                "type": "idea",
                "rule_score": float(rec["rule_score"]),
                "similarity_score": float(rec["similarity_score"]),
                "final_score": float(rec["final_score"]),
                "rank": rec.get("rank"),
                "created_at": rec["created_at"]
            }
            
            if include_raw:
                item["raw"] = rec["ideas"]
                
            recommendations.append(item)
        
        # Process campaign recommendations
        for rec in campaign_recs.data:
            item = {
                "confidenceScores": float(rec["final_score"]),
                "CampaignId": rec["campaigns"]["id"],
            }
            
            if include_raw:
                item["raw"] = rec["campaigns"]
                
            recommendations.append(item)
        
        # Sort by final score
        recommendations.sort(key=lambda x: (x["rank"] or 999, -x["final_score"]))
        
        return jsonify({
            "challenge_id": challenge_id,
            "Matches [ ]": recommendations[:limit]
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Keep existing endpoints for backward compatibility
import time

@app.route("/recommendations/idea/<challenge_id>", methods=["GET"])
def get_recommendations_idea(challenge_id: str):
    """Get saved idea recommendations for a specific challenge (grouped format)"""
    try:
        if not is_valid_uuid(challenge_id):
            return jsonify({"error": "Invalid challenge_id format"}), 400

        limit = request.args.get("limit", 10, type=int)
        include_raw = request.args.get("include_raw", "false").lower() == "true"

        start_time = time.time()

        # Fetch top-N idea recommendations (joined with ideas table)
        idea_recs = (
            supabase.table("idea_recommendations")
            .select("*, ideas!inner(*)")
            .eq("challenge_id", challenge_id)
            .order("final_score", desc=True)
            .limit(limit)
            .execute()
        )

        # Aggregate into arrays
        idea_ids = []
        confidence_scores = []
        raw_ideas = []  # only returned if include_raw=true

        for rec in idea_recs.data or []:
            # Collect ids and scores
            idea_ids.append(rec["ideas"]["id"])
            confidence_scores.append(float(rec["final_score"]))
            if include_raw:
                raw_ideas.append(rec["ideas"])

        matches_obj = {
            "challengeId": challenge_id,
            "ideaIds": idea_ids,
            "confidenceScores": confidence_scores,
        }
        if include_raw:
            matches_obj["rawIdeas"] = raw_ideas

        processing_time = f"{int((time.time() - start_time) * 1000)}ms"

        return jsonify({
            "matches": [matches_obj],
            "processingTime": processing_time
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

import time

@app.route("/recommendations/campaign/<challenge_id>", methods=["GET"])
def get_recommendations_campaign(challenge_id: str):
    """Get saved campaign recommendations for a specific challenge (grouped format)"""
    try:
        if not is_valid_uuid(challenge_id):
            return jsonify({"error": "Invalid challenge_id format"}), 400

        limit = request.args.get("limit", 10, type=int)
        include_raw = request.args.get("include_raw", "false").lower() == "true"

        start_time = time.time()

        # Fetch top-N campaign recommendations (joined with campaigns table)
        campaign_recs = (
            supabase.table("campaign_recommendations")
            .select("*, campaigns!inner(*)")
            .eq("challenge_id", challenge_id)
            .order("final_score", desc=True)
            .limit(limit)
            .execute()
        )

        # Aggregate into arrays
        campaign_ids = []
        confidence_scores = []
        raw_campaigns = []  # only returned if include_raw=true

        for rec in (campaign_recs.data or []):
            campaign_ids.append(rec["campaigns"]["id"])
            confidence_scores.append(float(rec["final_score"]))
            if include_raw:
                raw_campaigns.append(rec["campaigns"])

        matches_obj = {
            "challengeId": challenge_id,
            "campaignIds": campaign_ids,
            "confidenceScores": confidence_scores,
        }
        if include_raw:
            matches_obj["rawCampaigns"] = raw_campaigns

        processing_time = f"{int((time.time() - start_time) * 1000)}ms"

        return jsonify({
            "matches": [matches_obj],
            "processingTime": processing_time
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route("/challenges/<challenge_id>/conditions", methods=["GET"])
def get_challenge_conditions(challenge_id: str):
    """Get conditions for a specific challenge"""
    try:
        if not is_valid_uuid(challenge_id):
            return jsonify({"error": "Invalid challenge_id format"}), 400
        
        result = supabase.table("challenges")\
            .select("id, title, type")\
            .eq("id", challenge_id)\
            .single()\
            .execute()
        
        if not result.data:
            return jsonify({"error": "Challenge not found"}), 404
        
        # Get conditions from separate table
        conditions_result = supabase.table("challenge_conditions")\
            .select("*")\
            .eq("challenge_id", challenge_id)\
            .execute()
        
        challenge_data = result.data
        challenge_data["conditions"] = conditions_result.data
        
        return jsonify({
            "success": True,
            "challenge": challenge_data
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/stats", methods=["GET"])
def get_stats():
    """Get system statistics"""
    try:
        # Count entities
        challenges_count = supabase.table("challenges").select("id", count="exact").execute()
        ideas_count = supabase.table("ideas").select("id", count="exact").execute()
        campaigns_count = supabase.table("campaigns").select("id", count="exact").execute()
        
        # Count recommendations
        idea_recs_count = supabase.table("challenge_idea_recommendations")\
            .select("id", count="exact").execute()
        campaign_recs_count = supabase.table("challenge_campaign_recommendations")\
            .select("id", count="exact").execute()
        
        return jsonify({
            "success": True,
            "stats": {
                "challenges": challenges_count.count,
                "ideas": ideas_count.count,
                "campaigns": campaigns_count.count,
                "idea_recommendations": idea_recs_count.count,
                "campaign_recommendations": campaign_recs_count.count,
                "total_recommendations": idea_recs_count.count + campaign_recs_count.count
            }
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)