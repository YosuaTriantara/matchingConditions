from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd


class SimilarityEngine:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(max_features=5000)
        self.fitted = False

    def fit(self, texts: list[str]):
        self.vectorizer.fit(texts)
        self.fitted = True

    def compute(self, text1: str, text2: str) -> float:
        if not self.fitted:
            raise RuntimeError("Vectorizer not fitted. Call fit() first.")
        X = self.vectorizer.transform([text1, text2])
        sim = cosine_similarity(X[0], X[1])[0][0]
        return float(sim)


def build_similarity_engine(candidates: pd.DataFrame, challenges: pd.DataFrame) -> SimilarityEngine:
    texts = []
    for df in [candidates, challenges]:
        for _, row in df.iterrows():
            s = " ".join([
                str(row.get("title", "")),
                str(row.get("description", "")),
                " ".join(row.get("tags", []) if isinstance(row.get("tags"), list) else [])
            ])
            texts.append(s)
    engine = SimilarityEngine()
    engine.fit(texts)
    return engine 

