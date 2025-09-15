import operator
import re
from typing import Any, Dict, List
import pandas as pd

# map operators to functions
OPS = {
    ">=": operator.ge,
    ">": operator.gt,
    "<=": operator.le,
    "<": operator.lt,
    "=": operator.eq,
    "==": operator.eq,
    "!=": operator.ne,
    "contains": lambda a, b: str(b).lower() in str(a).lower(),
}

def _get_candidate_value(candidate: Dict[str, Any], field: str):
    """Try several fallbacks to read a field in candidate dict/row.
    Handles case differences and common alternative names.
    """
    if field in candidate:
        return candidate[field]
    
    # try case-insensitive
    lower_map = {k.lower(): k for k in candidate.keys()}
    if field.lower() in lower_map:
        return candidate[lower_map[field.lower()]]

    # common aliases
    aliases = {
        "supervotes": ["superVotes", "supervotes", "super_vote", "super_vote_count"],
        "votes": ["votes", "vote", "voters"],
        "feedbacks": ["feedbacks", "comments", "responses"],
        "supports": ["supports", "support", "supporters"],
        "title": ["title", "name"],
        "description": ["description", "desc", "content"],
    }
    
    for canon, keys in aliases.items():
        if field.lower() == canon:
            for k in keys:
                if k in candidate:
                    return candidate[k]
                if k.lower() in lower_map:
                    return candidate[lower_map[k.lower()]]
    
    # fallback 0 or empty
    print(f"⚠️ Field '{field}' not found in candidate. Available fields: {list(candidate.keys())}")
    return 0


def _text_contains_any_all(text, words, operator_mode="any"):
    """Check if text contains words based on operator mode"""
    if not text or not words:
        return False
        
    text_lower = str(text).lower()
    words = [str(w).lower().strip() for w in words if w]

    if operator_mode == "all":
        return all(w in text_lower for w in words if w)
    else:  # default = any
        return any(w in text_lower for w in words if w)


def evaluate_condition(condition: Dict[str, Any], candidate: Dict[str, Any]) -> bool:
    """Evaluate a single condition against a candidate"""
    try:
        kind = condition.get("kind")
        print(f"  Evaluating condition: {condition}")
        
        if kind == "numeric":
            field = condition.get("field")
            op_str = condition.get("operator")
            target_value = condition.get("value", 0)
            
            if not op_str or not field:
                print(f"    Missing operator ({op_str}) or field ({field})")
                return False
                
            op_func = OPS.get(op_str)
            if not op_func:
                print(f"    Unknown operator: {op_str}")
                return False
                
            candidate_value = _get_candidate_value(candidate, field)
            
            try:
                candidate_num = float(candidate_value) if candidate_value is not None else 0
                target_num = float(target_value)
            except (ValueError, TypeError):
                print(f"    Cannot convert to numbers: candidate='{candidate_value}', target='{target_value}'")
                return False
                
            result = op_func(candidate_num, target_num)
            print(f"    Numeric: {candidate_num} {op_str} {target_num} = {result}")
            return result

        elif kind == "words":
            words = condition.get("words", [])
            operator_mode = condition.get("operator", "any")
            
            if not words:
                print("    No words specified")
                return False
                
            # Combine title, description, and tags
            candidate_text_parts = []
            
            title = candidate.get("title", "")
            if title:
                candidate_text_parts.append(str(title))
                
            description = candidate.get("description", "")
            if description:
                candidate_text_parts.append(str(description))
                
            tags = candidate.get("tags", [])
            if isinstance(tags, list):
                candidate_text_parts.extend([str(tag) for tag in tags])
            elif isinstance(tags, str):
                candidate_text_parts.append(tags)
                
            combined_text = " ".join(candidate_text_parts)
            
            result = _text_contains_any_all(combined_text, words, operator_mode)
            print(f"    Words: '{words}' in '{combined_text[:100]}...' (mode={operator_mode}) = {result}")
            return result
            
        elif kind == "field":
            # Direct field comparison
            field = condition.get("field")
            op_str = condition.get("operator", "=")
            target_value = condition.get("value")
            
            if not field:
                return False
                
            candidate_value = _get_candidate_value(candidate, field)
            op_func = OPS.get(op_str)
            
            if not op_func:
                return False
                
            result = op_func(str(candidate_value).lower(), str(target_value).lower())
            print(f"    Field: {field} ({candidate_value}) {op_str} {target_value} = {result}")
            return result
            
        else:
            print(f"    Unknown condition kind: {kind}")
            return False
            
    except Exception as e:
        print(f"    Error evaluating condition: {e}")
        return False


def rule_based_match(challenge: Dict[str, Any], candidates_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Basic rule-based matching for backward compatibility
    """
    return rule_based_match_improved(
        challenge, 
        candidates_df,
        min_conditions_passed=0,  # Accept any match
        min_score_threshold=0.0   # No minimum threshold
    )


def rule_based_match_improved(challenge: Dict[str, Any], candidates_df: pd.DataFrame, 
                             min_conditions_passed: int = 1,
                             min_score_threshold: float = 0.1) -> List[Dict[str, Any]]:
    """
    Improved rule-based matching dengan filtering yang lebih ketat
    """
    conditions = challenge.get("conditions", []) or []
    total_conditions = len(conditions)
    results = []
    
    challenge_id = challenge.get("id", "unknown")
    print(f"\n🔍 Processing challenge: {challenge_id}")
    print(f"📋 Total conditions: {total_conditions}")
    
    if total_conditions == 0:
        print("⚠️ No conditions found - returning all candidates with score 1.0")
        # If no conditions, return all candidates with perfect score
        for _, row in candidates_df.iterrows():
            candidate = row.to_dict()
            results.append({
                "id": candidate.get("id"),
                "title": candidate.get("title", "")[:50],
                "passed_conditions": 0,
                "total_conditions": 0,
                "score": 1.0,  # Perfect score when no conditions
                "condition_details": [],
                "raw": candidate,
            })
        return results

    for idx, row in candidates_df.iterrows():
        candidate = row.to_dict()
        candidate_id = candidate.get("id", f"idx_{idx}")
        
        print(f"\n  👤 Candidate: {candidate_id} | {candidate.get('title', '')[:40]}")
        
        passed_conditions = 0
        condition_details = []
        
        for cond_idx, condition in enumerate(conditions):
            print(f"    Condition {cond_idx + 1}/{total_conditions}:")
            
            try:
                is_passed = evaluate_condition(condition, candidate)
                if is_passed:
                    passed_conditions += 1
                    
                condition_details.append({
                    "condition": condition,
                    "passed": is_passed
                })
                
                print(f"    ✅ Passed: {is_passed}")
                
            except Exception as e:
                print(f"    ❌ Error evaluating condition: {e}")
                condition_details.append({
                    "condition": condition,
                    "passed": False
                })

        # Calculate score
        score = (passed_conditions / total_conditions) if total_conditions > 0 else 1.0
        
        print(f"  📊 Score: {passed_conditions}/{total_conditions} = {score:.2f}")
        
        # Determine if should include
        should_include = False
        
        if total_conditions == 0:
            should_include = True
            reason = "no conditions"
        elif passed_conditions >= min_conditions_passed and score >= min_score_threshold:
            should_include = True
            reason = f"passed {passed_conditions} conditions (min: {min_conditions_passed}) and score {score:.2f} (min: {min_score_threshold})"
        elif score >= 0.5:  # High score override
            should_include = True
            reason = f"high score override: {score:.2f}"
        else:
            reason = f"failed: only {passed_conditions} conditions passed, score {score:.2f}"
            
        print(f"  🎯 Include: {should_include} ({reason})")
            
        if should_include:
            results.append({
                "id": candidate_id,
                "title": candidate.get("title", "")[:50],
                "passed_conditions": passed_conditions,
                "total_conditions": total_conditions,
                "score": score,
                "condition_details": condition_details,
                "raw": candidate,
            })

    # Sort by score (descending) and engagement
    def sort_key(x):
        raw = x.get("raw", {})
        
        # Primary: rule score
        rule_score = x["score"]
        
        # Secondary: engagement metrics
        votes = raw.get("votes", 0) or 0
        supports = raw.get("supports", 0) or 0  
        comments = raw.get("comments", 0) or 0
        
        try:
            votes = float(votes)
            supports = float(supports)
            comments = float(comments)
        except (ValueError, TypeError):
            votes = supports = comments = 0
            
        engagement = votes + supports + (comments * 0.5)
        
        return (rule_score, engagement)

    results_sorted = sorted(results, key=sort_key, reverse=True)
    
    print(f"\n✅ Final results: {len(results_sorted)} candidates matched")
    for i, result in enumerate(results_sorted[:5]):  # Show top 5
        print(f"  {i+1}. {result['id']} | {result['title']} | Score: {result['score']:.2f}")
    
    return results_sorted


def filter_by_similarity(matches: List[Dict], challenge_text: str, engine, 
                        min_similarity: float = 0.1) -> List[Dict]:
    """
    Filter matches by minimum similarity threshold
    """
    filtered = []
    
    print(f"\n🔍 Filtering {len(matches)} matches by similarity (min: {min_similarity})")
    
    for match in matches:
        candidate_text = " ".join([
            str(match["raw"].get("title", "")),
            str(match["raw"].get("description", "")),
            " ".join(match["raw"].get("tags", []) if isinstance(match["raw"].get("tags"), list) else [])
        ])
        
        try:
            sim_score = engine.compute(challenge_text, candidate_text)
            
            if sim_score >= min_similarity:
                match["similarity_score"] = sim_score
                filtered.append(match)
                print(f"  ✅ {match['id']}: similarity {sim_score:.3f}")
            else:
                print(f"  ❌ {match['id']}: similarity {sim_score:.3f} < {min_similarity}")
                
        except Exception as e:
            print(f"  ❌ {match['id']}: Error computing similarity: {e}")
    
    print(f"✅ {len(filtered)} matches passed similarity filter")
    return filtered