# For the pure rule-based pipeline ranking is done in rule_based.py (score = passed/total)
# This module can host additional ranking heuristics later (e.g. boost by votes or tag-overlap).

def combine_scores_improved(rule_score: float, similarity_score: float, 
                          engagement_score: float = 0.0,
                          alpha: float = 0.5, beta: float = 0.3, gamma: float = 0.2) -> float:
    """
    Enhanced scoring yang memperhitungkan rule, similarity, dan engagement
    alpha + beta + gamma harus = 1.0
    """
    if abs(alpha + beta + gamma - 1.0) > 0.01:
        raise ValueError("alpha + beta + gamma must equal 1.0")
    
    # Normalize engagement score (0-1 range)
    normalized_engagement = min(engagement_score / 100.0, 1.0)  # assuming max 100 votes/comments
    
    final_score = (
        alpha * rule_score + 
        beta * similarity_score + 
        gamma * normalized_engagement
    )
    
    return min(final_score, 1.0)  # Cap at 1.0