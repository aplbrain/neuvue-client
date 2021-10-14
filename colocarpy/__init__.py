#!/usr/bin/env python3
"""
# colocarpy.Colocard

## Sieveing
The `sieve` keyword in many of the below functions refers to an arbitrary
sieve that the end developer can pass according to the mongoose spec.

================================================================================

Use http://patorjk.com/software/taag/#p=display&h=0&f=ANSI%20Shadow&t=Volumes
to render large fonts.

================================================================================

Copyright 2018 The Johns Hopkins University Applied Physics Laboratory.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from typing import Any, Callable, List, Optional

import datetime
import json
import warnings
import configparser
import os

import networkx as nx
from networkx.readwrite import json_graph
import pandas as pd
import requests

from . import utils
from . import version
from . import validator

__version__ = version.__version__

class Colocard:
    """
    colocarpy.Colocard abstracts the interfaces to interact with Colocard.

    See colocarpy/__init__.py for more documentation.

    """

    def __init__(self, url: str, **kwargs) -> None:
        """
        Create a new colocard client.

        Arguments:
            url (str): The qualified location (including protocol) of the server.

        """
        # TODO: Add google auth layer
        # config = configparser.ConfigParser()

        # auth_method = ""
        # if "username" in kwargs and "password" in kwargs:
        #     auth_method = "Inline Arguments"
        #     self._username = kwargs["username"]
        #     self._password = kwargs["password"]
        # elif ("COLOCARD_USERNAME" in os.environ) and (
        #     "COLOCARD_PASSWORD" in os.environ
        # ):
        #     auth_method = "Environment Variables"
        #     self._username = os.environ["COLOCARD_USERNAME"]
        #     self._password = os.environ["COLOCARD_PASSWORD"]
        # else:
        #     try:
        #         config.read(os.path.expanduser("~/.colocarpy/.colocarpy"))
        #         auth_method = "Config File"
        #         self._username = config["CONFIG"]["username"]
        #         self._password = config["CONFIG"]["password"]
        #     except:
        #         raise ValueError("No authentication (username/password) provided.")

        # try:
        #     self._token = self._get_authorization_token()["access_token"]
        # except KeyError:
        #     raise ValueError(f"Authorization failed with method [{auth_method}].")

        self._url = url.rstrip("/")
        self._custom_headers: dict = {}
        if "headers" in kwargs:
            self._custom_headers.update(kwargs["headers"])

    @property
    def _headers(self) -> dict:
        headers = {
            "content-type": "application/json"
        }
        headers.update(self._custom_headers)
        return headers

    def url(self, suffix: str = "") -> str:
        """
        Construct a FQ URL.

        Arguments:
            suffix (str): The endpoint to access.

        Returns:
            str: The fully qualified URL.

        """
        return self._url + "/" + suffix.lstrip("/")

    def dtype_columns(self, datatype: str) -> List[str]:
        """
        Get a list of columns for a datatype.
        """
        return {
            "graph": [
                "active",
                "author",
                "decisions",
                "metadata",
                "namespace",
                "parent",
                "structure",
                "submitted",
                "volume",
                "graph",
            ],
            "volume": [
                "__v",
                "active",
                "author",
                "bounds",
                "metadata",
                "name",
                "namespace",
                "resolution",
                "uri",
            ],
            "question": [
                "__v",
                "active",
                "artifacts",
                "assignee",
                "author",
                "closed",
                "created",
                "instructions",
                "metadata",
                "namespace",
                "opened",
                "priority",
                "status",
                "volume",
            ],
            "node": [
                "active",
                "author",
                "coordinate",
                "created",
                "decisions",
                "metadata",
                "namespace",
                "submitted",
                "type",
                "volume",
            ],
            "point": [
                "__v",
                "active",
                "author",
                "coordinate",
                "resolution",
                "created",
                "metadata",
                "namespace",
                "submitted",
                "type"
            ],
            "task": [
                "__v",
                "active",
                "assignee",
                "author",
                "closed",
                "created",
                "instructions",
                "metadata",
                "namespace",
                "opened",
                "priority",
                "points"
                "status",
                "neuron_status"
            ]
        }[datatype]

    def _try_request(self, send_req: Callable[[], Any]) -> Any:
        res = send_req()
        # if res.status_code == 401:
        #     self._set_authorization_token()
        #     res = send_req()
        return res

    def depaginate(
        self,
        datatype: str,
        sieve: dict,
        populate: List[str] = None,
        select: List[str] = None,
        sort: List[str] = None,
        limit: int = None,
    ) -> list:
        depaginated: list = []
        page = 0
        data_remaining = True
        while data_remaining:
            new = self._get_data_by_page(
                datatype, sieve, page, populate=populate, select=select, sort=sort
            )
            page += 1
            if not new:
                data_remaining = False
            depaginated += new
            if limit and len(depaginated) >= limit:
                return depaginated[:limit]
        return depaginated

    def _get_data_by_page(
        self,
        datatype: str,
        sieve: dict,
        page: int = 0,
        populate: List[str] = None,
        select: List[str] = None,
        sort: List[str] = None,
    ):
        params = {
            "p": page,
            "q": json.dumps(sieve),
            "populate": ",".join(populate) if populate else None,
            "select": ",".join(select) if select else None,
            "sort": ",".join(sort) if sort else None,
        }
        res = self._try_request(
            lambda: requests.get(
                self.url(datatype), headers=self._headers, params=params
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(
                f"Unable to retrieve from page {page} of type {datatype}"
            ) from e
        return res.json()

    def _raise_for_status(self, res) -> None:
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            try:
                body = res.json()
            except Exception as ee:
                raise ee from e
            else:
                if "message" in body:
                    raise RuntimeError(body["message"]) from e
                raise e

    """
     ██████╗ ██████╗  █████╗ ██████╗ ██╗  ██╗███████╗
    ██╔════╝ ██╔══██╗██╔══██╗██╔══██╗██║  ██║██╔════╝
    ██║  ███╗██████╔╝███████║██████╔╝███████║███████╗
    ██║   ██║██╔══██╗██╔══██║██╔═══╝ ██╔══██║╚════██║
    ╚██████╔╝██║  ██║██║  ██║██║     ██║  ██║███████║
     ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚══════╝
    """

    def get_graph(
        self,
        graph_id: str,
        populate_volume: bool = False,
        regenerate_graph: bool = True,
    ) -> dict:
        """
        Get a single graph by its ID.

        Arguments:
            graph_id (str): The ID of the graph to retrieve.
            populate_volume (bool): Whether to populate the graph's volume id with the actual volume object.
            regenerate_graph (bool): Whether to rehydrate networkx graphs from the structure field.

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/graphs/{graph_id}"),
                headers=self._headers,
                params={"populate": "volume" if populate_volume else None},
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to get graph {graph_id}") from e

        result = res.json()
        if regenerate_graph:
            result["graph"] = json_graph.node_link_graph(result["structure"])
        return result

    def delete_graph(self, graph_id: str) -> str:
        """
        Delete a single graph.

        Arguments:
            graph_id (str): The ID of the graph to delete.

        Returns:
            str

        """
        res = self._try_request(
            lambda: requests.delete(
                self.url(f"/graphs/{graph_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to delete graph {graph_id}") from e
        return graph_id

    def get_graphs(
        self,
        sieve: dict = None,
        populate_volume: bool = False,
        regenerate_graph: bool = True,
        limit: int = None,
        active_default: bool = True,
    ) -> list:
        """
        Get a list of graphs.

        Automatically converts the `structure` component to a graph object
        in networkx format, which is stored in the `graph` key of the object.

        Arguments:
            sieve (dict): See sieve documentation.
            populate_volume (bool): Whether to populate the graphs' volume id with their corresponding volume object.
            regenerate_graph (bool): Whether to rehydrate networkx graphs from the structure field.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default
        populate = ["volume"] if populate_volume else None

        try:
            depaginated_graphs = self.depaginate(
                "graphs", sieve, populate=populate, limit=limit
            )
        except Exception as e:
            raise RuntimeError("Failed to get graphs") from e
        else:
            res = pd.DataFrame(depaginated_graphs)
            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("graph"))

            res.set_index("_id", inplace=True)
            res.submitted = pd.to_datetime(res.submitted, unit="ms")
            if regenerate_graph:
                res["graph"] = res.structure.map(json_graph.node_link_graph)

            return res

    def post_graph(
        self,
        volume: str,
        graph: nx.Graph,
        author: str,
        namespace: str,
        validate: bool = True,
    ):
        """
        Post a new graph to the database.

        Arguments:
            volume: str,
            graph: nx.Graph,
            author: str,
            namespace: str,
            validate: bool = True

        Returns:
            dict: Graph, as inserted

        """

        if validate:
            if not isinstance(graph, nx.Graph):
                raise ValueError(f"Graph must be a networkx.Graph.")

            if not all(["coordinate" in n for _, n in graph.nodes(True)]):
                raise ValueError("All nodes must have a `coordinate`.")

        req_obj = {
            "volume": volume,
            "structure": json_graph.node_link_data(graph),
            "author": author,
            "namespace": namespace,
            "__v": 0,
        }

        res = self._try_request(
            lambda: requests.post(
                self.url("/graphs/"), data=json.dumps(req_obj), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Unable to post graph") from e

        return res.json()

    """
    ██╗   ██╗ ██████╗ ██╗     ██╗   ██╗███╗   ███╗███████╗███████╗
    ██║   ██║██╔═══██╗██║     ██║   ██║████╗ ████║██╔════╝██╔════╝
    ██║   ██║██║   ██║██║     ██║   ██║██╔████╔██║█████╗  ███████╗
    ╚██╗ ██╔╝██║   ██║██║     ██║   ██║██║╚██╔╝██║██╔══╝  ╚════██║
     ╚████╔╝ ╚██████╔╝███████╗╚██████╔╝██║ ╚═╝ ██║███████╗███████║
      ╚═══╝   ╚═════╝ ╚══════╝ ╚═════╝ ╚═╝     ╚═╝╚══════╝╚══════╝
    """

    def get_volume(self, volume_id: str) -> dict:
        """
        Get a single volume by its ID.

        Arguments:
            volume_id (str): The ID of the volume to retrieve

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url("/volumes/{}".format(volume_id)), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to get volume {volume_id}") from e
        return res.json()

    def delete_volume(self, volume_id: str) -> str:
        """
        Delete a single volume.

        Arguments:
            volume_id (str): The ID of the volume to delete

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.delete(
                self.url(f"/volumes/{volume_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to delete volume {volume_id}") from e
        return volume_id

    def get_volumes(
        self, sieve: dict = None, limit: int = None, active_default: bool = True
    ):
        """
        Get a list of volumes.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default
        try:
            depaginated_volumes = self.depaginate("volumes", sieve, limit=limit)
        except Exception as e:
            raise RuntimeError("Failed to get volumes") from e
        else:
            res = pd.DataFrame(depaginated_volumes)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("volume"))

            res.set_index("_id", inplace=True)
            res.uri = res.uri.map(utils.unpack_uri)
            return res

    def post_volume(
        self,
        name: str,
        uri: str,
        bounds: List[List[int]],
        resolution: int,
        author: str,
        namespace: str,
        metadata: dict = None,
        validate: bool = True,
    ):
        """
        Post a new volume to the database.

        Arguments:
            name (str)
            uri (str)
            bounds (List[List[int]])
            resolution (int)
            author (str)
            namespace (str)
            metadata (dict = None)
            validate (bool = True)

        Returns:
            dict: Volume, as inserted

        """
        if metadata is None:
            metadata = {}

        if validate:
            if not isinstance(resolution, int):
                raise ValueError(f"Resolution [{resolution}] must be an int.")

            try:
                utils.unpack_uri(uri)
            except:
                raise ValueError(f"URI [{uri}] is malformed.")

            if (
                not isinstance(bounds, list)
                or len(bounds) != 2
                or not isinstance(bounds[0], list)
                or not isinstance(bounds[1], list)
                or not (len(bounds[0]) == len(bounds[1]) == 3)
            ):
                raise ValueError("Bounds must be of type Number[2, 3].")

        volume = {
            "active": True,
            "bounds": bounds,
            "metadata": metadata,
            "author": author,
            "name": name,
            "namespace": namespace,
            "resolution": resolution,
            "uri": uri,
            "__v": 0,
        }

        res = self._try_request(
            lambda: requests.post(
                self.url("/volumes"), data=json.dumps(volume), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Unable to post volume") from e
        return res.json()

    """
     ██████╗ ██╗   ██╗███████╗███████╗████████╗██╗ ██████╗ ███╗   ██╗███████╗
    ██╔═══██╗██║   ██║██╔════╝██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║██╔════╝
    ██║   ██║██║   ██║█████╗  ███████╗   ██║   ██║██║   ██║██╔██╗ ██║███████╗
    ██║▄▄ ██║██║   ██║██╔══╝  ╚════██║   ██║   ██║██║   ██║██║╚██╗██║╚════██║
    ╚██████╔╝╚██████╔╝███████╗███████║   ██║   ██║╚██████╔╝██║ ╚████║███████║
     ╚══▀▀═╝  ╚═════╝ ╚══════╝╚══════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝
    """

    def get_question(self, question_id: str) -> dict:
        """
        Get a single question by its ID.

        Arguments:
            question_id (str): The ID of the question to retrieve

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/questions/{question_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to get question {question_id}") from e

        return res.json()

    def get_next_question(self, assignee: str, namespace: str) -> dict:
        """
        Get the next question for a user.

        First checks for an open question; then sorts by highest to lowest
        priority of unopened questions.

        Arguments:
            assignee (str): The username of the assignee
            namespace (str): The app for which the question was assigned

        Returns:
            dict

        """
        query = json.dumps(
            {
                "assignee": assignee,
                "namespace": namespace,
                "active": True,
                "status": "opened",
            }
        )
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/questions"), headers=self._headers, params={"q": query}
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Unable to get opened questions") from e

        r = res.json()

        if len(r):
            return r[0]

        query = json.dumps(
            {
                "assignee": assignee,
                "namespace": namespace,
                "active": True,
                "status": "pending",
            }
        )
        res = self._try_request(
            lambda: requests.get(
                self.url("/questions"),
                headers=self._headers,
                params={"q": query, "sort": "-priority"},
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Unable to get opened questions") from e

        r = res.json()

        return r[0] if r else None

    def delete_question(self, question_id: str) -> str:
        """
        Delete a single question.

        Arguments:
            question_id (str): The ID of the question to delete

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.delete(
                self.url(f"/questions/{question_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to delete question {question_id}") from e
        return question_id

    def get_questions(
        self, sieve: dict = None, limit: int = None, active_default: bool = True
    ):
        """
        Get a list of questions.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default
        try:
            depaginated_questions = self.depaginate("questions", sieve, limit=limit)
        except Exception as e:
            raise RuntimeError("Unable to get questions") from e
        else:
            res = pd.DataFrame(depaginated_questions)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("question"))

            res.set_index("_id", inplace=True)
            res.created = pd.to_datetime(res.created, unit="ms")
            res.opened = pd.to_datetime(res.opened, unit="ms")
            res.closed = pd.to_datetime(res.closed, unit="ms")
            return res

    def post_question(
        self,
        volume: str,
        author: str,
        assignee: str,
        priority: int,
        namespace: str,
        instructions: dict,
        metadata: dict = None,
        validate: bool = True,
    ):
        """
        Post a new question to the database.

        Arguments:
            volume (str)
            author (str)
            assignee (str)
            priority (int)
            namespace (str)
            instructions (dict)
            metadata (dict = None)
            validate (bool = True)

        Returns:
            dict

        """
        if metadata is None:
            metadata = {}

        if not isinstance(priority, int):
            raise ValueError(f"Priority [{priority}] must be an integer.")

        if validate:
            try:
                self.get_volume(volume)
            except Exception as e:
                raise RuntimeError(f"Failed to validate volume [{volume}]") from e

            # App-specific validation
            if namespace == "breadcrumbs":
                if "graph" not in instructions:
                    raise ValueError(
                        "instructions.graph must be provided to breadcrumbs questions."
                    )
                else:
                    try:
                        self.get_graph(instructions["graph"])
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to validate graph {instructions['graph']} existance"
                        ) from e

        question = {
            "active": True,
            "closed": None,
            "metadata": metadata,
            "opened": None,
            "status": "pending",
            "volume": volume,
            "priority": priority,
            "author": author,
            "assignee": assignee,
            "namespace": namespace,
            "instructions": instructions,
            "created": utils.date_to_ms(),
            "__v": 0,
        }

        res = self._try_request(
            lambda: requests.post(
                self.url("/questions"), data=json.dumps(question), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Failed to post question") from e
        return res.json()

    def post_question_broadcast(
        self,
        volume: str,
        author: str,
        assignees: List[str],
        priority: int,
        namespace: str,
        instructions: dict,
        metadata: dict = None,
        validate: bool = True,
    ):
        """
        Post a new question to the database for a given set of assignees.

        Arguments:
            volume (str)
            author (str)
            assignees (List[str])
            priority (int)
            namespace (str)
            instructions (dict)
            metadata (dict = None)
            validate (bool = True)

        Returns:
            List[dict]

        """
        if metadata is None:
            metadata = {}

        if not isinstance(priority, int):
            raise ValueError(f"Priority [{priority}] must be an integer.")

        if validate:
            try:
                self.get_volume(volume)
            except Exception as e:
                raise RuntimeError(f"Failed to validate volume [{volume}]") from e

            # App-specific validation
            if namespace == "breadcrumbs":
                if "graph" not in instructions:
                    raise ValueError(
                        "instructions.graph must be provided to breadcrumbs questions."
                    )
                else:
                    try:
                        self.get_graph(instructions["graph"])
                    except Exception as e:
                        raise RuntimeError(
                            f"Failed to validate graph {instructions['graph']} existance"
                        ) from e

        questions = []
        created = utils.date_to_ms()
        for a in assignees:
            questions.append(
                {
                    "active": True,
                    "closed": None,
                    "metadata": metadata,
                    "opened": None,
                    "status": "pending",
                    "volume": volume,
                    "priority": priority,
                    "author": author,
                    "assignee": a,
                    "namespace": namespace,
                    "instructions": instructions,
                    "created": created,
                    "__v": 0,
                }
            )

        res = self._try_request(
            lambda: requests.post(
                self.url("/questions"),
                data=json.dumps(questions),
                headers=self._headers,
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Failed to post question") from e
        return res.json()

    """
    ███╗   ██╗ ██████╗ ██████╗ ███████╗███████╗
    ████╗  ██║██╔═══██╗██╔══██╗██╔════╝██╔════╝
    ██╔██╗ ██║██║   ██║██║  ██║█████╗  ███████╗
    ██║╚██╗██║██║   ██║██║  ██║██╔══╝  ╚════██║
    ██║ ╚████║╚██████╔╝██████╔╝███████╗███████║
    ╚═╝  ╚═══╝ ╚═════╝ ╚═════╝ ╚══════╝╚══════╝
    """

    def get_node(self, node_id: str, populate_volume: bool = False) -> dict:
        """
        Get a single node by its ID.

        Arguments:
            node_id (str): The ID of the node to retrieve
            populate_volume (bool): Whether to populate the graph's volume id with the actual volume object.
        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/nodes/{node_id}"),
                headers=self._headers,
                params={"populate": "volume" if populate_volume else None},
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Failed to get node {node_id}") from e
        return res.json()

    def get_nodes(
        self,
        sieve: dict = None,
        populate_volume: bool = False,
        limit: int = None,
        active_default: bool = True,
    ):
        """
        Get a list of nodes.

        Arguments:
            sieve (dict): See sieve documentation.
            populate_volume (bool): Whether to populate the nodes' volume id with the actual volume object.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default
        populate = ["volume"] if populate_volume else None

        try:
            depaginated_nodes = self.depaginate(
                "nodes", sieve, populate=populate, limit=limit
            )
        except Exception as e:
            raise RuntimeError("Failed to get nodes") from e
        else:
            res = pd.DataFrame(depaginated_nodes)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("node"))

            res.set_index("_id", inplace=True)
            res.created = pd.to_datetime(res.created, unit="ms")
            res.submitted = pd.to_datetime(res.submitted, unit="ms")

            return res

    """
    ██████╗  ██████╗ ██╗███╗   ██╗████████╗███████╗
    ██╔══██╗██╔═══██╗██║████╗  ██║╚══██╔══╝██╔════╝
    ██████╔╝██║   ██║██║██╔██╗ ██║   ██║   ███████╗
    ██╔═══╝ ██║   ██║██║██║╚██╗██║   ██║   ╚════██║
    ██║     ╚██████╔╝██║██║ ╚████║   ██║   ███████║
    ╚═╝      ╚═════╝ ╚═╝╚═╝  ╚═══╝   ╚═╝   ╚══════╝
    """
    
    def get_point(self, point_id: str) -> dict:
        """
        Get a single point by its ID.

        Arguments:
            point_id (str): The ID of the point to retrieve
        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/points/{point_id}"),
                headers=self._headers,
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Failed to get point {point_id}") from e
        return res.json()

    def get_points(
        self,
        sieve: dict = None,
        limit: int = None,
        active_default: bool = True,
    ):
        """
        Get a list of points.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default

        try:
            depaginated_points = self.depaginate(
                "points", sieve, limit=limit
            )
        except Exception as e:
            raise RuntimeError("Failed to get points") from e
        else:
            res = pd.DataFrame(depaginated_points)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("points"))

            res.set_index("_id", inplace=True)
            res.created = pd.to_datetime(res.created, unit="ms")
            res.submitted = pd.to_datetime(res.submitted, unit="ms")

            return res

    def post_point(
        self,
        coordinate: List[int],
        author: str,
        namespace: str,
        type: str, 
        resolution: int = 0,
        metadata: dict = None,
        validate: bool = True,
        validator: object = validator.Minnie65Validator
    ):
        """
        Post a new point to the database.

        Arguments:
            coordinate (List[int])
            author (str)
            namespace (str)
            type (str)
            resolution (int = 0)
            metadata (dict = None)
            validate (bool = True)

        Returns:
            dict: Point, as inserted

        """
        if metadata is None:
            metadata = {}

        if validate:
            if not isinstance(resolution, int):
                raise ValueError(f"Resolution [{resolution}] must be an int.")

            if (
                not isinstance(coordinate, list)
                or len(coordinate) != 3
                or not validator.validate_point(coordinate)
            ):
                raise ValueError(f"Validation failed for coordinate {coordinate}.")
        created = utils.date_to_ms()
        point = {
            "active": True,
            "coordinate": coordinate,
            "author": author,
            "namespace": namespace,
            "type": type, 
            "resolution": resolution,
            "metadata": metadata,
            "created": created,
            "__v": 0,
        }

        res = self._try_request(
            lambda: requests.post(
                self.url("/points"), data=json.dumps(point), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Unable to post point") from e
        return res.json()

    """
    ████████╗ █████╗ ███████╗██╗  ██╗███████╗
    ╚══██╔══╝██╔══██╗██╔════╝██║ ██╔╝██╔════╝
       ██║   ███████║███████╗█████╔╝ ███████╗
       ██║   ██╔══██║╚════██║██╔═██╗ ╚════██║
       ██║   ██║  ██║███████║██║  ██╗███████║
       ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝
    """
    
    def get_task(self, task_id: str) -> dict:
        """
        Get a single task by its ID.

        Arguments:
            task_id (str): The ID of the task to retrieve

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/tasks/{task_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to get task {task_id}") from e

        return res.json()


    def get_next_task(self, assignee: str, namespace: str) -> dict:
        """
        Get the next task for a user.

        First checks for an open tasks; then sorts by highest to lowest
        priority of unopened tasks.

        Arguments:
            assignee (str): The username of the assignee
            namespace (str): The app/sprint for which the task was assigned

        Returns:
            dict

        """
        query = json.dumps(
            {
                "assignee": assignee,
                "namespace": namespace,
                "active": True,
                "status": "opened",
            }
        )
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/tasks"),
                 headers=self._headers,
                 params={"q": query}
                 #params={"q": query, "sort": "-priority"}, #can there be multiple open tasks
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Unable to get opened tasks") from e

        r = res.json()

        if len(r):
            return r[0]

        query = json.dumps(
            {
                "assignee": assignee,
                "namespace": namespace,
                "active": True,
                "status": "pending",
            }
        )
        res = self._try_request(
            lambda: requests.get(
                self.url("/tasks"),
                headers=self._headers,
                params={"q": query, "sort": "-priority"},
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Unable to get opened tasks") from e

        r = res.json()

        return r[0] if r else None

    
    def delete_task(self, task_id: str) -> str:
        """
        Delete a single task.

        Arguments:
            task_id (str): The ID of the task to delete

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.delete(
                self.url(f"/task/{task_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to delete task {task_id}") from e
        return task_id

    def get_tasks(
        self, sieve: dict = None, limit: int = None, active_default: bool = True
    ):
        """
        Get a list of tasks.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default
        try:
            depaginated_tasks = self.depaginate("tasks", sieve, limit=limit)
        except Exception as e:
            raise RuntimeError("Unable to get tasks") from e
        else:
            res = pd.DataFrame(depaginated_tasks)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("tasks"))

            res.set_index("_id", inplace=True)
            res.created = pd.to_datetime(res.created, unit="ms")
            res.opened = pd.to_datetime(res.opened, unit="ms")
            res.closed = pd.to_datetime(res.closed, unit="ms")
            return res

    def post_task(
        self,
        points: List[str],
        author: str,
        assignee: str,
        priority: int,
        namespace: str,
        instructions: dict,
        metadata: dict = None,
        validate: bool = True,
    ):
        """
        Post a new task to the database.

        Arguments:
            points List(str)
            author (str)
            assignee (str)
            priority (int)
            namespace (str)
            instructions (dict)
            metadata (dict = None)
            validate (bool = True)

        Returns:
            dict

        """
        if metadata is None:
            metadata = {}

        if not isinstance(priority, int):
            raise ValueError(f"Priority [{priority}] must be an integer.")

        if validate:
            for point in points:
                try:
                    self.get_point(point)
                except Exception as e:
                    raise RuntimeError(f"Failed to validate point [{point}]") from e

        task = {
            "active": True,
            "closed": None,
            "metadata": metadata,
            "opened": None,
            "status": "pending",
            "points": points,
            "priority": priority,
            "author": author,
            "assignee": assignee,
            "namespace": namespace,
            "instructions": instructions,
            "created": utils.date_to_ms(),
            "__v": 0,
        }

        res = self._try_request(
            lambda: requests.post(
                self.url("/tasks"), data=json.dumps(task), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Failed to post task") from e
        return res.json()

    def post_task_broadcast(
        self,
        points: List[str],
        author: str,
        assignees: List[str],
        priority: int,
        namespace: str,
        instructions: dict,
        metadata: dict = None,
        validate: bool = True,
    ):
        """
        Post a new task to the database for a given set of assignees.

        Arguments:
            volume (str)
            author (str)
            assignees (List[str])
            priority (int)
            namespace (str)
            instructions (dict)
            metadata (dict = None)
            validate (bool = True)

        Returns:
            List[dict]

        """
        if metadata is None:
            metadata = {}

        if not isinstance(priority, int):
            raise ValueError(f"Priority [{priority}] must be an integer.")

        if validate:
            for point in points:
                try:
                    self.get_point(point)
                except Exception as e:
                    raise RuntimeError(f"Failed to validate point [{point}]") from e

        tasks = []
        created = utils.date_to_ms()
        for a in assignees:
            tasks.append(
                {
                    "active": True,
                    "closed": None,
                    "metadata": metadata,
                    "opened": None,
                    "status": "pending",
                    "points": points,
                    "priority": priority,
                    "author": author,
                    "assignee": a,
                    "namespace": namespace,
                    "instructions": instructions,
                    "created": created,
                    "__v": 0,
                }
            )

        res = self._try_request(
            lambda: requests.post(
                self.url("/tasks"),
                data=json.dumps(tasks),
                headers=self._headers,
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Failed to post task") from e
        return res.json()

