from .word_count import WordCounter, count_line, IrisClientInternal, IrisObjectInternal, IrisContextInternal
from word_count.client import IrisContext, IrisClientWrapper, IrisGradContext, IrisObject, IrisOptimizer, IrisModel
__all__ = [
    "WordCounter", 
    "count_line", 
    "search_py", 
    "IrisContextInternal", 
    "IrisObjectInternal",
    "IrisClientInternal", 
    "IrisContext",
    "IrisGradContext",
    "IrisOptimizer",
    "IrisModel",
    "AsyncTest"
    ]


def search_py(path, needle):
    total = 0
    with open(path, "r") as f:
        for line in f:
            words = line.split(" ")
            for word in words:
                if word == needle:
                    total += 1
    return total
