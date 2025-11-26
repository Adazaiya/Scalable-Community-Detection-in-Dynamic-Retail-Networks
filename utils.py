# utils.py
import math
import numpy as np

def normalized_cooccurrence(count_ij, count_i, count_j):
    """co-occurrence / sqrt(count_i * count_j)"""
    denom = math.sqrt(max(count_i * count_j, 1))
    return count_ij / denom

def pmi_and_npmi(count_ij, count_i, count_j, total_orders):
    """Return (PMI, NPMI). Uses probabilities estimated by counts."""
    # probabilities
    p_i = count_i / total_orders
    p_j = count_j / total_orders
    p_ij = count_ij / total_orders
    if p_ij == 0 or p_i == 0 or p_j == 0:
        return None, None
    pmi = math.log(p_ij / (p_i * p_j))
    npmi = pmi / (-math.log(p_ij)) if p_ij < 1 else 0.0
    return pmi, npmi
