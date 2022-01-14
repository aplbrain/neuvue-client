import datetime
import requests
import backoff
import networkx as nx
import os
import json 

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

def is_json(value):
    try:
        json.loads(value)
        return True
    except:
        return False

def get_caveclient_token():
    # Get the authorization token from caveclient
    token_file = os.path.expanduser('~/.cloudvolume/secrets/cave-secret.json')
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            return json.load(f).get("token")
    
@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def post_to_state_server(state: str, json_state_server:str, json_state_server_token:str): 
    """Posts JSON string to state server

    Args:
        state (str): NG State string
    
    Returns:
        str: url string
    """

    headers = {
        'content-type': 'application/json',
        'Authorization': f"Bearer {json_state_server_token}"
    }

    # Post! 
    resp = requests.post(json_state_server, data=state, headers=headers)

    if resp.status_code != 200:
        raise Exception("POST Unsuccessful")
    
    # Response will contain the URL for the state you just posted
    return str(resp.json())

@backoff.on_exception(backoff.expo, Exception, max_tries=3)
def get_from_state_server(url: str, json_state_server_token): 
    """Gets JSON state string from state server

    Args:
        url (str): json state server link
    Returns:
        (str): JSON String 
    """
    headers = {
        'content-type': 'application/json',
        'Authorization': f"Bearer {json_state_server_token}"
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        raise Exception("GET Unsuccessful")
    
    # TODO: Make sure its JSON String
    return resp.text