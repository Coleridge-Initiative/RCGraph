'''
Some examples for LSH
'''

from hashlib import sha1
import numpy as np
from datasketch.minhash import MinHash
from datasketch.weighted_minhash import WeightedMinHashGenerator
from datasketch.lsh import MinHashLSH
from datasketch import MinHashLSHEnsemble, MinHash

set1 = set(['minhash', 'is', 'a', 'probabilistic', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'datasets'])
set2 = set(['minhash', 'is', 'a', 'probability', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'documents'])
set3 = set(['minhash', 'is', 'probability', 'data', 'structure', 'for',
            'estimating', 'the', 'similarity', 'between', 'documents'])

v1 = np.random.uniform(1, 10, 10)
v2 = np.random.uniform(1, 10, 10)
v3 = np.random.uniform(1, 10, 10)

def eg1():
    m1 = MinHash(num_perm=128)
    m2 = MinHash(num_perm=128)
    m3 = MinHash(num_perm=128)
    for d in set1:
        m1.update(d.encode('utf8'))
    for d in set2:
        m2.update(d.encode('utf8'))
    for d in set3:
        m3.update(d.encode('utf8'))

    # Create LSH index
    lsh = MinHashLSH(threshold=0.5, num_perm=128)
    lsh.insert("m2", m2)
    lsh.insert("m3", m3)
    result = lsh.query(m1)
    print("Approximate neighbours with Jaccard similarity > 0.5", result)

def eg2():
    mg = WeightedMinHashGenerator(10, 5)
    m1 = mg.minhash(v1)
    m2 = mg.minhash(v2)
    m3 = mg.minhash(v3)
    print("Estimated Jaccard m1, m2", m1.jaccard(m2))
    print("Estimated Jaccard m1, m3", m1.jaccard(m3))
    # Create LSH index
    lsh = MinHashLSH(threshold=0.1, num_perm=5)
    lsh.insert("m2", m2)
    lsh.insert("m3", m3)
    result = lsh.query(m1)
    print("Approximate neighbours with weighted Jaccard similarity > 0.1", result)


def ensemble():


    set1 = set(["cat", "dog", "fish", "cow"])
    set2 = set(["cat", "dog", "fish", "cow", "pig", "elephant", "lion", "tiger",
                "wolf", "bird", "human"])
    set3 = set(["cat", "dog", "car", "van", "train", "plane", "ship", "submarine",
                "rocket", "bike", "scooter", "motorcyle", "SUV", "jet", "horse"])

    # Create MinHash objects
    m1 = MinHash(num_perm=128)
    m2 = MinHash(num_perm=128)
    m3 = MinHash(num_perm=128)
    for d in set1:
        m1.update(d.encode('utf8'))
    for d in set2:
        m2.update(d.encode('utf8'))
    for d in set3:
        m3.update(d.encode('utf8'))

    # Create an LSH Ensemble index with threshold and number of partition
    # settings.
    lshensemble = MinHashLSHEnsemble(threshold=0.1, num_perm=128,
                                     num_part=32)

    # Index takes an iterable of (key, minhash, size)
    lshensemble.index([("m2", m2, len(set2)), ("m3", m3, len(set3))])

    # Check for membership using the key
    print("m2" in lshensemble)
    print("m3" in lshensemble)
    print("m1" in lshensemble)

    # Using m1 as the query, get an result iterator
    print("Sets with containment > 0.8:")
    for key in lshensemble.query(m1, len(set1)):
        print(key)


if __name__ == "__main__":
    #eg1()
    print("********")
    #eg2()
    ensemble()