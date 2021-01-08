from .client import WordCounter, count_line, IrisClientInternal, IrisObjectInternal, IrisContextInternal, IrisObjectId
from client.ext import IrisContext, IrisClientWrapper, IrisObject, IrisModel, RemoteTensor, AsyncIterator, IrisConfig, remote, RemoteFunction
__all__ = [
    "WordCounter",
    "count_line",
    "search_py",
    "IrisContextInternal",
    "IrisObjectInternal",
    "IrisClientInternal",
    "IrisContext",
    "IrisModel",
    "AsyncTest",
    "IrisObjectId",
    "retrieve_args",
    "RemoteTensor",
    "AsyncIterator",
    "IrisConfig",
    "remote",
    "RemoteFunction"
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
