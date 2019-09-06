import itertools

EOS = object()


def map_with_lookahead(it, if_one=None, if_many=None):
    """
    It's like normal map: creates a new generator by applying a function to every
    element of the original generator, but it applies `if_one` transform for
    single element sequences and `if_many` transform for multi-element sequences.

    If iterators supported `len` it would be equivalent to the code below::

        proc = if_many if len(it) > 1 else if_one
        return map(proc, it)

    :param it: Sequence to iterate over
    :param if_one: Function to apply for single element sequences
    :param if_many: Function to apply for multi-element sequences

    """
    if_one = if_one or (lambda x: x)
    if_many = if_many or (lambda x: x)

    it = iter(it)
    p1 = list(itertools.islice(it, 2))
    proc = if_many if len(p1) > 1 else if_one

    for v in itertools.chain(iter(p1), it):
        yield proc(v)


def qmap(func, q, eos_marker=EOS):
    """ Converts queue to an iterator.

    For every `item` in the `q` that is not `eos_marker`, `yield proc(item)`

    Takes care of calling `.task_done()` on every item extracted from the queue.
    """
    while True:
        item = q.get(block=True)
        if item is eos_marker:
            q.task_done()
            break
        else:
            try:
                yield func(item)
            finally:
                q.task_done()


def it2q(its, q, eos_marker=EOS):
    """ Convert iterator into a Queue

        [1, 2, 3] => [1, 2, 3, eos_marker]
    """
    try:
        for x in its:
            q.put(x, block=True)
    finally:
        q.put(eos_marker, block=True)
