# Backend/classifier/similarity.py

import math
from typing import List

class SimilarityScorer:
    """
    Computes similarity metrics between embeddings.
    """
    
    @staticmethod
    def cosine_similarity(vec1: List[float], vec2: List[float]) -> float:
        """
        Computes the cosine similarity between two vectors.
        """
        if not vec1 or not vec2:
            return 0.0
            
        if len(vec1) != len(vec2):
            return 0.0
            
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm_a = math.sqrt(sum(a * a for a in vec1))
        norm_b = math.sqrt(sum(b * b for b in vec2))
        
        if norm_a == 0.0 or norm_b == 0.0:
            return 0.0
            
        return dot_product / (norm_a * norm_b)
