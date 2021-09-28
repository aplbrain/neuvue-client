import datetime

import networkx as nx

from typing import Optional


def structure_to_nx(structure: dict) -> nx.Graph:
    """
    Convert a `structure` key to a networkx.Graph.

    Arguments:
        structure (dict): Node-link form dictionary

    Returns:
        nx.Graph

    """
    g = nx.Graph()
    for n in structure["nodes"]:
        if "id" not in n and "_id" not in n:
            return g
        else:
            nid = n.get("id", n.get("_id"))
        g.add_node(nid, pos=[n["coordinate"][0], n["coordinate"][1]], **n)
    for e in structure["links"]:
        g.add_edge(e["source"], e["target"])
    return g


def date_to_ms(date: datetime.datetime = None) -> int:
    if date is None:
        date = datetime.datetime.now()
    return int(datetime.datetime.timestamp(date) * 1000)


def ms_to_date(ms: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ms / 1000.0)


def _unpack_boss_uri(boss_uri: str) -> dict:
    """
    Unpack a Boss URI.

    TODO: Brittle!
    """
    components = list(reversed(boss_uri.split("://")[1].split("/")))
    collection = components[2]
    experiment = components[1]
    channel = components[0]
    return {
        "type": "bossdb",
        "collection": collection,
        "experiment": experiment,
        "channel": channel,
    }


def unpack_uri(uri: str) -> dict:
    """
    Unpack a URI and return a dictionary of its attributes.

    Arguments:
        uri (str): The URI to unpack

    Returns:
        dict: The unpacked URI

    """
    uri_unpackers = {
        # Currently, only one unpacker
        "bossdb": _unpack_boss_uri
    }
    uri_type = uri.split("://")[0]
    if uri_type not in uri_unpackers:
        return {"URI": uri}
    return uri_unpackers[uri_type](uri)
