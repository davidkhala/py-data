from typing import Literal

from scipy.stats import pearsonr


def corr(x, y, method: Literal['pearson', 'spearman']):
    match method:
        case 'pearson':
            return pearsonr(x, y) # tuple(corr_coef, p_value)
        case 'spearman':
            ...
