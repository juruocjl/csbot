"""Run without starting NoneBot: python scripts/check_streaming_bm25.py."""
import ast
from collections import Counter
import importlib.util
import math
from pathlib import Path
import random
import tracemalloc
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("ranking", ROOT / "plugins/chat_history/ranking.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

# Compare against the existing implementation without importing the bot/plugins.
tree = ast.parse((ROOT / "plugins/chat_history/__init__.py").read_text())
reference = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_bm25_rank")
namespace = dict(Counter=Counter, math=math, BM25_K1=1.5, BM25_B=.75)
exec(compile(ast.Module(body=[reference], type_ignores=[]), "reference", "exec"), namespace)


class RankingTests(unittest.TestCase):
    def test_exact_scores_order_and_ties(self):
        rng = random.Random(2026)
        for size in (0, 1, 100, 5000):
            docs = [[rng.choice("abcdef") for _ in range(rng.randrange(50))] for _ in range(size)]
            terms = ["a", "c", "a", "absent"]
            scores = namespace["_bm25_rank"](terms, docs)
            expected = sorted(((i, score) for i, score in enumerate(scores) if score > 0),
                              key=lambda pair: (pair[1], pair[0] % 5), reverse=True)[:20]
            with module.StreamingBM25(terms, 20) as ranker:
                for i, doc in enumerate(docs):
                    ranker.add(i, i % 5, doc)
                self.assertEqual(ranker.finish(), expected)

    def test_large_corpus_bounded_heap(self):
        tracemalloc.start()
        with module.StreamingBM25(["query"], 20) as ranker:
            for i in range(50_000):
                ranker.add(i, i, ["query", f"unique-{i}"] * 500)
            self.assertEqual(len(ranker.finish()), 20)
            self.assertEqual(ranker.count, 50_000)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(f"50000-document ranking peak Python allocations: {peak / 1024**2:.2f} MiB")
        self.assertLess(peak, 8 * 1024**2)

    def test_empty_tokens_and_cleanup(self):
        with module.StreamingBM25(["absent"]) as ranker:
            ranker.add(1, 1, [])
            self.assertEqual(ranker.finish(), [])
        self.assertTrue(ranker.spool.closed)


if __name__ == "__main__":
    unittest.main()
