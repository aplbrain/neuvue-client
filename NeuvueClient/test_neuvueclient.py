import neuvueclient as Client
from neuvueclient import NeuvueQueue
from networkx import Graph

import random
import unittest

# TODO URL
NEUVUEQUEUE_URL = "https://colocard.thebossdev.io"


class TestNeuvueClientGraphs(unittest.TestCase):
    def test_converts_graph(self):
        C = NeuvueQueue(NEUVUEQUEUE_URL)

        result = C.get_graphs({"author": "j6m8", "active": True})

        self.assertEqual(type(result.iloc[0].structure), dict)
        self.assertEqual(type(result.iloc[0].graph), Graph)

    def test_empty_list(self):
        C = NeuvueQueue(NEUVUEQUEUE_URL)

        result = C.get_graphs(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )

        self.assertListEqual(list(result.columns), C.dtype_columns("graph"))


class TestNeuvueClientVolumes(unittest.TestCase):
    def test_empty_list(self):
        C = NeuvueQueue(NEUVUEQUEUE_URL)

        result = C.get_volumes(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )

        self.assertListEqual(list(result.columns), C.dtype_columns("volume"))


class TestNeuvueClientQuestions(unittest.TestCase):
    def test_empty_list(self):
        C = NeuvueQueue(NEUVUEQUEUE_URL)

        result = C.get_questions(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )

        self.assertListEqual(list(result.columns), C.dtype_columns("question"))


class TestNeuvueClientNodes(unittest.TestCase):
    def test_empty_list(self):
        C = NeuvueQueue(NEUVUEQUEUE_URL)

        result = C.get_nodes(
            {"author": f"random-user-{random.randint(1000, 2000)}", "active": False}
        )
        self.assertListEqual(list(result.columns), C.dtype_columns("node"))

