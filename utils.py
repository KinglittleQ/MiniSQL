import pickle
from config import protocol


def load(path):
    try:
        ret = pickle.load(open(path, 'rb'))
        return ret
    except EOFError:
        return []


def dump(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f, protocol=protocol)


def get_size(obj):
    return len(pickle.dumps(obj, protocol=protocol))
