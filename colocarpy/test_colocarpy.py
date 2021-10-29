import colocarpy
from colocarpy import Colocard
from networkx import Graph

import random
import unittest

COLOCARD_URL = "https://colocard.thebossdev.io"


class TestColocarpyGraphs(unittest.TestCase):
    def test_converts_graph(self):
        C = Colocard(COLOCARD_URL)

        result = C.get_graphs({"author": "j6m8", "active": True})

        self.assertEqual(type(result.iloc[0].structure), dict)
        self.assertEqual(type(result.iloc[0].graph), Graph)

    def test_empty_list(self):
        C = Colocard(COLOCARD_URL)

        result = C.get_graphs(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )

        self.assertListEqual(list(result.columns), C.dtype_columns("graph"))


class TestColocarpyVolumes(unittest.TestCase):
    def test_empty_list(self):
        C = Colocard(COLOCARD_URL)

        result = C.get_volumes(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )

        self.assertListEqual(list(result.columns), C.dtype_columns("volume"))


class TestColocarpyQuestions(unittest.TestCase):
    def test_empty_list(self):
        C = Colocard(COLOCARD_URL)

        result = C.get_questions(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )

        self.assertListEqual(list(result.columns), C.dtype_columns("question"))


class TestColocarpyNodes(unittest.TestCase):
    def test_empty_list(self):
        C = Colocard(COLOCARD_URL)

        result = C.get_nodes(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )
        self.assertListEqual(list(result.columns), C.dtype_columns("node"))

