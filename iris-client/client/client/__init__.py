from .client import WordCounter, count_line, IrisClientInternal, IrisObjectInternal, IrisContextInternal, IrisObjectId
from client.ext import IrisContext, IrisClientWrapper, IrisGradContext, IrisObject, IrisOptimizer, IrisModel, retrieve_args
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
    "AsyncTest",
    "IrisObjectId",
    "retrieve_args"
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