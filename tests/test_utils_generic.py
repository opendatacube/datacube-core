from queue import Queue
from datacube.utils.generic import (
    qmap,
    it2q,
    map_with_lookahead,
    thread_local_cache,
)


def test_map_with_lookahead():
    def if_one(x):
        return 'one' + str(x)

    def if_many(x):
        return 'many' + str(x)

    assert list(map_with_lookahead(iter([]), if_one, if_many)) == []
    assert list(map_with_lookahead(iter([1]), if_one, if_many)) == [if_one(1)]
    assert list(map_with_lookahead(range(5), if_one, if_many)) == list(map(if_many, range(5)))
    assert list(map_with_lookahead(range(10), if_one=if_one)) == list(range(10))
    assert list(map_with_lookahead(iter([1]), if_many=if_many)) == [1]


def test_qmap():
    q = Queue(maxsize=100)
    it2q(range(10), q)
    rr = [x for x in qmap(str, q)]
    assert rr == [str(x) for x in range(10)]
    q.join()  # should not block


def test_thread_local_cache():
    name = "test_0123394"
    v = {}

    assert thread_local_cache(name, v) is v
    assert thread_local_cache(name) is v
    assert thread_local_cache(name, purge=True) is v
    assert thread_local_cache(name, 33) == 33
    assert thread_local_cache(name, purge=True) == 33

    assert thread_local_cache("no_such_key", purge=True) is None
    assert thread_local_cache("no_such_key", 111, purge=True) == 111
