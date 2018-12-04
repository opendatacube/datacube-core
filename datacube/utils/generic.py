import itertools


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
