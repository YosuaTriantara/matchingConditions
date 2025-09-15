import os
import time
import pandas as pd
from src.getData import load_and_save_normalized, save_campaign_recommendation, save_idea_recommendation
from src.preprocessing import preprocess_dataframe
from src.matching import filter_candidates_by_type
from src.ruledBased import rule_based_match_improved
from src.similarity import build_similarity_engine
from src.ranking import combine_scores_improved
from src.utils import normalize_tags

IDEAS_URL = os.environ.get("IDEAS_URL", "https://favbackend-dev.vercel.app/api/yos/ideas/list")
CAMPAIGNS_URL = os.environ.get("CAMPAIGNS_URL", "https://favbackend-dev.vercel.app/api/yos/campaigns/list")
CHALLENGES_URL = os.environ.get("CHALLENGES_URL", "https://favbackend-dev.vercel.app/api/yos/challenges/list")

def process_recommendations_optimized(
    ideas_url=IDEAS_URL,
    campaigns_url=CAMPAIGNS_URL,
    challenges_url=CHALLENGES_URL,
    challenge_id=None,
    save_to_db=True,
    min_score=0.1,
    limit=10,
    match_type="both"  # "campaigns", "ideas", or "both"
):
    """
    Optimized processing function that returns data in the requested JSON format
    """
    start_time = time.time()
    
    print("🚀 Starting optimized recommendation pipeline...")
    
    # 1. Load and preprocess data (single pass)
    ideas, campaigns, challenges = load_and_save_normalized(ideas_url, campaigns_url, challenges_url)
    ideas = preprocess_dataframe(ideas)
    campaigns = preprocess_dataframe(campaigns)
    challenges = preprocess_dataframe(challenges)
    
    # Filter specific challenge if requested
    if challenge_id:
        challenges = challenges[challenges['id'] == challenge_id]
        if challenges.empty:
            raise ValueError(f"Challenge {challenge_id} not found")
    
    print(f"📊 Loaded: {len(ideas)} ideas, {len(campaigns)} campaigns, {len(challenges)} challenges")
    
    # 2. Build similarity engine (single pass)
    all_candidates = pd.concat([ideas, campaigns], ignore_index=True)
    engine = build_similarity_engine(all_candidates, challenges)
    print("🔍 Similarity engine built")
    
    # 3. Initialize results
    campaign_matches = []
    idea_matches = []
    total_saved = 0
    
    # 4. Process each challenge
    for _, ch_row in challenges.iterrows():
        challenge = ch_row.to_dict()
        cid = challenge.get("id")
        
        print(f"\n--- Processing challenge: {cid} | {challenge.get('title', '')[:50]}")
        
        # Get candidates
        candidates = filter_candidates_by_type(challenge, ideas, campaigns)
        if candidates.empty:
            print("⚠️ No candidates found")
            continue
        
        # Apply rule-based matching
        matched = rule_based_match_improved(
            challenge, 
            candidates,
            min_conditions_passed=1,
            min_score_threshold=min_score
        )
        
        if not matched:
            print("⚠️ No matches after rule-based filtering")
            continue
        
        print(f"✅ {len(matched)} candidates matched")
        
        # Prepare challenge text for similarity
        challenge_text = " ".join([
            str(challenge.get("title", "")),
            str(challenge.get("description", "")),
            " ".join(normalize_tags(challenge.get("tags", [])))
        ])
        
        # Separate campaigns and ideas with scoring
        campaign_recs = []
        idea_recs = []
        
        for r in matched:
            try:
                candidate_text = " ".join([
                    str(r["raw"].get("title", "")),
                    str(r["raw"].get("description", "")),
                    " ".join(normalize_tags(r["raw"].get("tags", [])))
                ])
                
                # Calculate similarity score
                sim_score = engine.compute(challenge_text, candidate_text)
                
                # Calculate engagement score
                engagement = (r["raw"].get("votes", 0) or 0) + (r["raw"].get("supports", 0) or 0)
                
                # Calculate final score
                final_score = combine_scores_improved(r["score"], sim_score, engagement)
                
                rec_data = {
                    "id": r["id"],
                    "rule_score": r["score"],
                    "similarity_score": sim_score,
                    "final_score": final_score,
                    "raw": r["raw"]
                }
                
                # Determine type based on presence of campaign-specific fields
                is_campaign = any(field in r["raw"] for field in 
                                ["trigger_type", "preorder_price", "supports"])
                
                if is_campaign:
                    campaign_recs.append(rec_data)
                else:
                    idea_recs.append(rec_data)
                
                # Save to database if requested
                if save_to_db:
                    if is_campaign:
                        if save_campaign_recommendation(cid, r["id"], r["score"], sim_score, final_score):
                            total_saved += 1
                    else:
                        if save_idea_recommendation(cid, r["id"], r["score"], sim_score, final_score):
                            total_saved += 1
                            
            except Exception as e:
                print(f"    ❌ Error processing candidate {r.get('id', 'unknown')}: {e}")
                continue
        
        # Sort by final score (descending)
        campaign_recs.sort(key=lambda x: x["final_score"], reverse=True)
        idea_recs.sort(key=lambda x: x["final_score"], reverse=True)
        
        # Create campaign matches in requested format
        if (match_type in ["campaigns", "both"]) and campaign_recs:
            limited_campaigns = campaign_recs[:limit]
            campaign_match = {
                "challengeId": cid,
                "campaignIds": [r["id"] for r in limited_campaigns],
                "similarityScore": [round(r["similarity_score"], 3) for r in limited_campaigns],
                "ruleScore": [round(r["rule_score"], 3) for r in limited_campaigns],
                "finalScore": [round(r["final_score"], 3) for r in limited_campaigns]
            }
            campaign_matches.append(campaign_match)
            print(f"  📋 Campaign match created with {len(limited_campaigns)} recommendations")
        
        # Create idea matches in requested format  
        if (match_type in ["ideas", "both"]) and idea_recs:
            limited_ideas = idea_recs[:limit]
            idea_match = {
                "challengeId": cid,
                "ideaIds": [r["id"] for r in limited_ideas],  # Fixed: was "ideasIds"
                "similarityScore": [round(r["similarity_score"], 3) for r in limited_ideas],
                "ruleScore": [round(r["rule_score"], 3) for r in limited_ideas],
                "finalScore": [round(r["final_score"], 3) for r in limited_ideas]
            }
            idea_matches.append(idea_match)
            print(f"  💡 Idea match created with {len(limited_ideas)} recommendations")
    
    # Calculate processing time
    processing_time = f"{int((time.time() - start_time) * 1000)}ms"
    
    # Return results based on match_type
    results = {
        "processingTime": processing_time,
        "summary": {
            "challenges_processed": len(challenges),
            "recommendations_saved": total_saved if save_to_db else "not_saved"
        }
    }
    
    if match_type == "campaigns":
        results["matches"] = campaign_matches
    elif match_type == "ideas":
        results["matches"] = idea_matches
    else:  # both
        results["campaign_matches"] = campaign_matches
        results["idea_matches"] = idea_matches
    
    return results

def main():
    """
    Main function with optimized single-pass processing
    """
    print("🚀 Starting optimized recommendation pipeline...")
    
    try:
        # Process with both types
        results = process_recommendations_optimized(
            match_type="both",
            save_to_db=True,
            min_score=0.1,
            limit=10
        )
        
        # Print summary
        print("\n" + "="*60)
        print("📊 PIPELINE SUMMARY")
        print("="*60)
        print(f"⏱️ Processing time: {results['processingTime']}")
        print(f"🎯 Challenges processed: {results['summary']['challenges_processed']}")
        print(f"💾 Recommendations saved: {results['summary']['recommendations_saved']}")
        
        if "campaign_matches" in results:
            campaign_count = sum(len(match["campaignIds"]) for match in results["campaign_matches"])
            print(f"🏢 Total campaign recommendations: {campaign_count}")
            
        if "idea_matches" in results:
            idea_count = sum(len(match["ideaIds"]) for match in results["idea_matches"])
            print(f"💡 Total idea recommendations: {idea_count}")
        
        # Example output format
        print("\n" + "="*60)
        print("📋 SAMPLE OUTPUT FORMAT")
        print("="*60)
        
        if results.get("campaign_matches"):
            print("\nCampaign Matches Sample:")
            sample_campaign = results["campaign_matches"][0]
            print(f"  Challenge: {sample_campaign['challengeId']}")
            print(f"  Campaign IDs: {sample_campaign['campaignIds'][:3]}...")
            print(f"  Similarity Scores: {sample_campaign['similarityScore'][:3]}...")
            print(f"  Final Scores: {sample_campaign['finalScore'][:3]}...")
        
        if results.get("idea_matches"):
            print("\nIdea Matches Sample:")
            sample_idea = results["idea_matches"][0]
            print(f"  Challenge: {sample_idea['challengeId']}")
            print(f"  Idea IDs: {sample_idea['ideaIds'][:3]}...")
            print(f"  Similarity Scores: {sample_idea['similarityScore'][:3]}...")
            print(f"  Final Scores: {sample_idea['finalScore'][:3]}...")
        
        return results
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()