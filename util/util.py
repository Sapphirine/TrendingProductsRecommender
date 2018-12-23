import itertools


def get_phrases(query_terms):
    phrases = {}
    for category in [key for key in query_terms.keys() if key != 'general']:
        phrases[category] = [' '.join(pair) for pair in
                             itertools.product(query_terms['general'], query_terms[category])]
    return phrases


def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))
