"""Exact BM25 with a disk spool: corpus size does not determine heap usage."""
from collections import Counter
import heapq
import json
import math
import tempfile


class StreamingBM25:
    def __init__(self, terms, limit=10, k1=1.5, b=0.75):
        self.terms = tuple(dict.fromkeys(terms))
        self.term_set = set(self.terms)
        self.limit, self.k1, self.b = limit, k1, b
        self.count = self.total_length = 0
        self.df = Counter()
        # Anonymous temporary file, removed on close, including exceptions.
        self.spool = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")

    def add(self, key, end_time, tokens):
        frequencies = Counter(token for token in tokens if token in self.term_set)
        self.df.update(frequencies.keys())
        self.spool.write(json.dumps([key, end_time, self.count, len(tokens), frequencies]) + "\n")
        self.count += 1
        self.total_length += len(tokens)

    def finish(self):
        if not self.count or not self.total_length:
            return []
        avg = self.total_length / self.count
        idf = {term: math.log(1 + (self.count - self.df[term] + .5) / (self.df[term] + .5))
               for term in self.terms}
        self.spool.seek(0)
        best = []
        for line in self.spool:
            key, end_time, position, length, frequencies = json.loads(line)
            score = 0.0
            for term in self.terms:
                freq = frequencies.get(term, 0)
                if freq:
                    denominator = freq + self.k1 * (1 - self.b + self.b * length / avg)
                    score += idf[term] * (freq * (self.k1 + 1)) / denominator
            if score > 0:
                # Equal score/time preserves the original database row order.
                item = (score, end_time, -position, key)
                if len(best) < self.limit:
                    heapq.heappush(best, item)
                elif item > best[0]:
                    heapq.heapreplace(best, item)
        return [(key, score) for score, _, _, key in sorted(best, reverse=True)]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.spool.close()
