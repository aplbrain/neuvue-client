#!/usr/bin/env python3
"""
# neuvueclient.NeuvueQueue

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

import http
import ast
from typing import Any, Callable, Dict, List

import datetime
import json
import configparser
import os

import pandas as pd
import requests

from . import utils
from . import version

__version__ = version.__version__

class NeuvueQueue:
    """
    neuvueclient.NeuvueQueue abstracts the interfaces to interact with NeuvueQueue.

    See neuvueclient/__init__.py for more documentation.

    """
    def __init__(self, url: str, **kwargs) -> None:
        """
        Create a new neuvuequeue client.

        Arguments:
            url (str): The qualified location (including protocol) of the server.

        """
        self.config = configparser.ConfigParser()
        self._url = url.rstrip("/")
        self.queue_address = self._url.split('//')[1]
        # JSON State Server Info
        self._json_state_server = kwargs.get('json_state_server', "https://global.daf-apis.com/nglstate/post")
        self._json_state_server_token = kwargs.get('json_state_server_token', utils.get_caveclient_token())
        self._local = False
        if "token" in kwargs:
            self.auth_method = "Inline Arguments"
            self._refresh_token = kwargs["refresh_token"]
            self._access_token = kwargs["access_token"] 
        
        elif ("NEUVUEQUEUE_REFRESH_TOKEN" in os.environ) and ("NEUVUEQUEUE_ACCESS_TOKEN" in os.environ):
            self.auth_method = "Environment Variables"
            self._refresh_token = os.environ["NEUVUEQUEUE_REFRESH_TOKEN"]
            self._access_token = os.environ["NEUVUEQUEUE_ACCESS_TOKEN"] 
        elif kwargs.get('local', False):
            self._local = True
            self.auth_method = "Local (NO AUTH)"
        else:
            self.auth_method = "Config File"
            try:
                self.config.read(os.path.expanduser("~/.neuvuequeue/neuvuequeue.cfg"))
                self._refresh_token = self.config["CONFIG"]["refresh_token"]

            except:
                print("No tokens found. Please login. \n")
                self.login()

            self._refresh_authorization_token(self._refresh_token)
            self.config.read(os.path.expanduser("~/.neuvuequeue/neuvuequeue.cfg"))

            self._access_token = self.config["CONFIG"]["access_token"]

        print(f"Auth method: {self.auth_method}")
        self._custom_headers: dict = {}
        if "headers" in kwargs:
            self._custom_headers.update(kwargs["headers"])
    
    def login(self):
        """
        Generates a new authorization token and saves it to a config file.
        """

        link = "https://dev-oe-jgl7m.us.auth0.com/authorize?response_type=code&client_id=BdwlItpSZeMrd2ZJwaVrmn0VILYhmriK&redirect_uri=https://app.neuvue.io/token&scope=openid%20profile%20email%20offline_access&audience=https://queue.neuvue.io"
        
        # Verify code 
        code = input(f"Go to this link: \n {link} \n and log in using your google account, then copy the text the Token page here: ")
        
        # Make a request to neuvuequeue to get the authorization token
        conn = http.client.HTTPSConnection(self.queue_address)

        payload = "{\"code\":\"" + code + "\",\"code_type\":\"authorization\"}"

        headers = { 'content-type': "application/json" }
        conn.request("POST", "/auth/tokens", payload, headers)

        res = conn.getresponse()
        data = res.read()
        response = data.decode("utf-8")
        response_dict = ast.literal_eval(response)
        self.config['CONFIG'] = {'refresh_token': response_dict.get("refresh_token", ""),
                                 'access_token': response_dict.get("access_token", "")}
        try:
             os.mkdir(os.path.expanduser("~/.neuvuequeue"))
        except OSError:
            pass

        with open(os.path.expanduser("~/.neuvuequeue/neuvuequeue.cfg"), 'w') as configfile:
            self.config.write(configfile)
            
        print(f"Credentials saved to file at ~/.neuvuequeue/neuvuequeue.cfg, which will be read from now on \n \nNote:If your token doesn't work, you may be a first time user. If this is the case, please give your email to the Neuvue team, and they will give your account the necessary permission to make a token.")
        self._access_token = response_dict["access_token"]
        self._refresh_token = response_dict["refresh_token"]

    def _refresh_authorization_token(self, refresh: str):
        """
        Use the refresh token to generate a new one. 
        """
        conn = http.client.HTTPSConnection(self.queue_address)
        payload = "{\"code\":\"" + refresh + "\",\"code_type\":\"refresh\"}"

        headers = { 'content-type': "application/json" }
        conn.request("POST", "/auth/tokens", payload, headers)

        res = conn.getresponse()
        data = res.read()
        response = data.decode("utf-8")
        response_dict = ast.literal_eval(response)
        access_token = response_dict["access_token"]
        if self.auth_method == "Config File":
            self.config["CONFIG"]["access_token"] = access_token
            
            try:
                os.mkdir(os.path.expanduser("~/.neuvuequeue"))
            except OSError:
                pass

            with open(os.path.expanduser("~/.neuvuequeue/neuvuequeue.cfg"), 'w') as configfile:
                self.config.write(configfile)

        elif self.auth_method == "Environment Variables":
            os.environ["NEUVUEQUEUE_ACCESS_TOKEN"] = access_token
        self._access_token = access_token

    @property
    def _headers(self) -> dict:
        if self._local:
            headers = {
                "content-type": "application/json"
            }
        else:
            headers = {
                "content-type": "application/json",
                "Authorization": f"Bearer {self._access_token}"
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
                "type",
                "agents_status"
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
                "duration",
                "points",
                "status",
                "seg_id",
                "tags",
                "ng_state"
            ],
            "differ_stack": [
                "active",
                "task_id",
                "differ_stack"
            ],
            "agents": [
                "active",
                "endpoint",
                "seg_id",
                "nucleus_id",
                "merges",
                "metadata",
                "created",
                "namespace"
            ]
        }[datatype]

    def _try_request(self, send_req: Callable[[], Any]) -> Any:
        res = send_req()
        if (res.status_code == 401 or res.status_code == 500) and not self._local:
            self._refresh_authorization_token(self._refresh_token)
            res = send_req()
        return res

    def depaginate(
        self,
        datatype: str,
        sieve: dict,
        populate: List[str] = None,
        select: List[str] = None,
        sort: List[str] = None,
        limit: int = None,
        **kwargs
    ) -> list:
        depaginated: list = []
        page = 0
        data_remaining = True
        while data_remaining:
            new = self._get_data_by_page(
                datatype, sieve, page, populate=populate, select=select, sort=sort, **kwargs
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
        **kwargs
    ):

        # Get page size if user set it, otherwise set to 15000
        pageSize = kwargs.get("pageSize", 15000)

        params = {
            "p": page,
            "q": json.dumps(sieve),
            "populate": ",".join(populate) if populate else None,
            "select": ",".join(select) if select else None,
            "sort": ",".join(sort) if sort else None,
            "pageSize": pageSize
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
        sort: str = "",
        active_default: bool = True,
        **kwargs
    ):
        """
        Get a list of points.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            sort (str): attribute to sort by, default is _id. Add `-` to the beginning of the attribute name to
                        sort in descending order.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this
            pageSize (int: 500): Number of entries to return per page

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default

        try:
            depaginated_points = self.depaginate(
                "points", sieve, limit=limit, sort=[sort], **kwargs
            )
        except Exception as e:
            raise RuntimeError("Failed to get points") from e
        else:
            res = pd.DataFrame(depaginated_points)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("point"))

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
            "__v": 1,
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

    def patch_point(self, point_id: str, **kwargs):
        """
        Patch a single point. Only agents_status is currently patchable.
        
        Arguments:
            point_id (str): The ID of the point to patch
            kwargs (dict or str or int): The fields to modify. Only supports 
                - agents_status

        Returns:
            JSON
        """
        if not kwargs:
            print("WARNING: No valid kwargs provided in patch_task().")
            return 

        valid_kwargs = ['agents_status']
        for key, value in kwargs.items():
            if key not in valid_kwargs:
                print("WARNING: Key {key} does not exist in point attributes.")
            # Append metadata to existing entries

            stri = f"/points/{point_id}/{key}"
            
            # Include flag for status updates, if needed. 
            data = {key:value}

            res = self._try_request( 
                lambda: requests.patch(
                    self.url(stri), 
                    data=json.dumps(data),
                    headers=self._headers)
            )
            try:
                self._raise_for_status(res)
            except Exception as e:
                raise RuntimeError(f"Unable to patch point {point_id}") from e


    """
    ████████╗ █████╗ ███████╗██╗  ██╗███████╗
    ╚══██╔══╝██╔══██╗██╔════╝██║ ██╔╝██╔════╝
       ██║   ███████║███████╗█████╔╝ ███████╗
       ██║   ██╔══██║╚════██║██╔═██╗ ╚════██║
       ██║   ██║  ██║███████║██║  ██╗███████║
       ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝
    """
    
    def get_task(self, task_id: str, populate_points: bool = False, convert_states_to_json: bool = False) -> dict:
        """
        Get a single task by its ID.

        Arguments:
            task_id (str): The ID of the task to retrieve
            populate_points (bool = False): Populate points for the task object.
            convert_states_to_json (bool = False): Whether to convert a state url to JSON string
        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/tasks/{task_id}"), 
                headers=self._headers,
                params={"populate": "points" if populate_points else None}
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to get task {task_id}") from e
        if convert_states_to_json: 
            task = res.json()
            task['ng_state'] = utils.get_from_state_server(task['ng_state'], self._json_state_server_token)
            return task
        else:
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
        query = {
                "assignee": assignee,
                "namespace": namespace,
                "active": True,
                "status": "open",
            }
        sort = ['-priority']
        try:
            res = self.depaginate("tasks", query, sort=sort, limit=1)
        except Exception as e:
            raise RuntimeError("Unable to get opened tasks") from e

        if len(res):
            return res[0]

        query = {
                "assignee": assignee,
                "namespace": namespace,
                "active": True,
                "status": "pending",
            }
        try:
            res = self.depaginate("tasks", query, sort=sort, limit=1)
        except Exception as e:
            raise RuntimeError("Unable to get opened tasks") from e

        return res[0] if res else None

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
                self.url(f"/tasks/{task_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to delete task {task_id}") from e
        return task_id

    def get_tasks(
        self, 
        sieve: dict = None, 
        limit: int = None, 
        active_default: bool = True,
        populate_points: bool = False,
        sort: str = '',
        convert_states_to_json: bool = True,
        **kwargs
    ):
        """
        Get a list of tasks.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this
            populate_points (bool): Whether to populate the tasks' point ids with their corresponding point object.
            sort (str): attribute to sort by, default is task_id. Add `-` to the beginning of the attribute name to
                        sort in descending order.
            convert_states_to_json (bool): whether to convert ng_states to json strings
            pageSize (int: 500): Number of entries to return per page
        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default
        time_queries = [key for key in sieve.keys() if key in ['created', 'opened', 'closed']]
        if time_queries:
            for key in time_queries:
                if len(sieve[key]) > 1:
                    assert sieve[key]['$gt'] < sieve[key]['$lt'], "$gt argument must be less than $lt if both are used."
                for query in sieve[key].keys():
                    assert type(sieve[key][query]) == datetime.datetime, "Please enter a datetime.datetime object."
                    sieve[key][query] = round(sieve[key][query].timestamp()*1000)
        
        populate = ["points"] if populate_points else None
        try:
            depaginated_tasks = self.depaginate("tasks", sieve, populate=populate, limit=limit, sort=[sort], **kwargs)
        except Exception as e:
            raise RuntimeError("Unable to get tasks") from e
        else:
            res = pd.DataFrame(depaginated_tasks)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("task"))
            res.set_index("_id", inplace=True)
            if 'created' in res.columns:
                res.created = pd.to_datetime(res.created, unit="ms")
            if 'opened' in res.columns:
                res.opened = pd.to_datetime(res.opened, unit="ms")
            if 'closed' in res.columns:
                res.closed = pd.to_datetime(res.closed, unit="ms")

            # Convert states to JSON if they are in URL format 
            if convert_states_to_json and 'ng_state' in res.columns:
                
                def _convert_state(x):
                    try:
                        return utils.get_from_state_server(x, self._json_state_server_token)
                    except: 
                        return x

                res['ng_state'] = res['ng_state'].apply( _convert_state)
            return res

    def post_task(
        self,
        author: str,
        assignee: str,
        priority: int,
        namespace: str,
        instructions: dict,
        points: List[str] = None,
        duration: int = 0,
        metadata: dict = None,
        seg_id: str = None,
        ng_state: str = None,
        version: int = 1,
        post_state: bool = True,
        **kwargs
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
            seg_id (str = None)
            post_state (bool = True)

        Returns:
            dict

        """
        if metadata is None:
            metadata = {}

        # type check parameters
        if not isinstance(instructions, dict):
            raise ValueError(f"Instructions [{instructions}] must be a dictionary.")

        if (not isinstance(priority, int)) or (priority < 1):
            raise ValueError(f"Priority [{priority}] must be an integer greater than 1.")

        if not isinstance(duration, int):
            raise ValueError(f"Duration [{duration}] must be an integer.")

        if points is not None and not isinstance(points, list):
            raise ValueError(f"Points [{points}] must be a list of strings.")
                
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError(f"Metadata [{metadata}] must be a dict.")

        if seg_id is not None and not isinstance(seg_id, str):
            raise ValueError(f"Seg_id [{seg_id}] must be a string.")
        
        if (not isinstance(assignee, str)) or (not isinstance(assignee[0], str)):
            raise ValueError(f"Assignee [{assignee}] must be a string.")
        
        if not isinstance(namespace, str):
            raise ValueError(f"Namespace [{namespace}] must be a string.")
        
        if (post_state and 
            ng_state is not None and 
            utils.is_json(ng_state) and
            self._json_state_server_token is not None
            ):

            ng_state_url = utils.post_to_state_server(
                ng_state, 
                self._json_state_server, 
                self._json_state_server_token)

            metadata['base_state'] = ng_state_url
        else:
            ng_state_url = None
        
        task = {
            "active": True,
            "closed": None,
            "metadata": metadata,
            "opened": None,
            "status": "pending",
            "points": points,
            "priority": priority,
            "duration": duration,
            "author": author,
            "assignee": assignee,
            "namespace": namespace,
            "instructions": instructions,
            "created": utils.date_to_ms(),
            "seg_id": seg_id,
            "ng_state": ng_state_url if ng_state_url else ng_state,
            "__v": version,
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
        author: str,
        assignees: List[str],
        priority: int,
        namespace: str,
        instructions: dict,
        points: List[str] = None,
        duration: int = 0,
        metadata: dict = None,
        seg_id: str = None,
        ng_state: str = None,
        post_state: bool = True
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
            seg_id (str = None)
            ng_state (str = None)
            post_state (bool = True)

        Returns:
            List[dict]

        """
        if metadata is None:
            metadata = {}

        if not isinstance(author, str):
            raise ValueError(f"Author [{author}] must be a string.")
        
        if (not isinstance(assignees, list)) or (not isinstance(assignees[0], str)):
            raise ValueError(f"Assignees [{assignees}] must be a list of strings.")

        if (not isinstance(priority, int)) or (priority < 1):
            raise ValueError(f"Priority [{priority}] must be an integer greater than 1.")

        if not isinstance(namespace, str):
            raise ValueError(f"Namespace [{namespace}] must be a string.")

        if not isinstance(instructions, dict):
            raise ValueError(f"Instructions [{instructions}] must be a dict.")

        if not isinstance(duration, int):
            raise ValueError(f"Duration [{duration}] must be an integer.")

        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError(f"Metadata [{metadata}] must be a dict.")

        if seg_id is not None and not isinstance(seg_id, str):
            raise ValueError(f"Seg_id [{seg_id}] must be a string.")

        if not isinstance(post_state, bool):
            raise ValueError(f"Post_state [{post_state}] must be a bool.")

        if (post_state and 
            ng_state is not None and
            utils.is_json(ng_state) and 
            self._json_state_server_token is not None
            ):
            ng_state_url = utils.post_to_state_server(
                ng_state, 
                self._json_state_server, 
                self._json_state_server_token)
        else:
            ng_state_url = None

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
                    "duration": duration,
                    "author": author,
                    "assignee": a,
                    "namespace": namespace,
                    "instructions": instructions,
                    "created": created,
                    "seg_id": seg_id,
                    "ng_state": ng_state_url if ng_state_url else ng_state,
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

    def patch_task(self, task_id: str, author: str = None, overwrite_opened: bool = True, **kwargs):
        """
        Patch a single task. Iterates through each argument passed through kwargs and patches each.
        
        Example:
        > patch_task(
            task_id, 
            author,
            instructions = {"prompt": 'do a good job'}, 
            status='pending', 
            priority='100'
        )
        
        Arguments:
            task_id (str): The ID of the question to delete
            overwrite_opened (bool): whether to update the opened time when patching status. 
            kwargs (dict or str or int): The fields to modify. Only supports 
                - instruction
                - priority 
                - status
                - metadata
                - duration
                - ngstate

        Returns:
            JSON
        """
        if not kwargs:
            print("WARNING: No valid kwargs provided in patch_task().")
            return 

        valid_kwargs = self.dtype_columns("task")
        task = self.get_task(task_id)

        # If status or assignee is designated, patch metadata to include provenance too
        if 'assignee' in kwargs.keys() or 'status' in kwargs.keys():
            if not author:
                author = task["author"]
                print("WARNING: No author has been designated in patch_task() kwargs. Original author of task will be used to record this change.")

            if 'metadata' in kwargs.keys():
                kwargs['metadata']['provenance'] = utils.update_provenance(task, author, {k: v for k, v in kwargs.items() if k in ['assignee', 'status']})
            else:
                kwargs['metadata'] = {"provenance": utils.update_provenance(task, author, {k: v for k, v in kwargs.items() if k in ['assignee', 'status']})}

        for key, value in kwargs.items():
            if key not in valid_kwargs:
                print("WARNING: Key {key} does not exist in task attributes.")
            # Append metadata to existing entries
            if key == 'metadata':
                old_metadata = task['metadata']
                old_metadata.update(value)
                value = old_metadata

            stri = f"/tasks/{task_id}/{key}"
            
            # Include flag for status updates, if needed. 
            if overwrite_opened and key == 'status':
                data = {key:value, "overwrite_opened": True}
            else:
                data = {key:value}

            res = self._try_request( 
                lambda: requests.patch(
                    self.url(stri), 
                    data=json.dumps(data),
                    headers=self._headers)
            )
            try:
                self._raise_for_status(res)
            except Exception as e:
                raise RuntimeError(f"Unable to patch task {task_id}") from e

    def copy_task(self, task_id:str, author:str = None, **kwargs):
        """Copy a task based on its original task ID and replaces any attributes through 
        kwargs that are subsequently passed to post_task(). 
        
        For database reasons, you cannot copy a task from one namespace to another. 

        Args:
            task_id (str): task ID to be copied
            author (str): your username
        Raises:
            ValueError: raised when namespace is included in kwargs

        Returns:
            dict: Post response
        """
        if kwargs.get('namespace'):
            raise ValueError("Cannot copy a task and replace its namespace.")
        
        task = self.get_task(task_id)
        task.update(kwargs)
        if author:
            task.update({"author": author})
        else:
            print("WARNING: No author has been designated in copy_task() kwargs. Original author of task will be used to record this change.")
        
        # Create copy provenance
        task['metadata']['provenance'] = utils.create_new_provenance(task, copy=True)

        return self.post_task(**task, post_state=False)


    '''
    ██████╗ ██╗███████╗███████╗███████╗██████╗     ███████╗████████╗ █████╗  ██████╗██╗  ██╗███████╗
    ██╔══██╗██║██╔════╝██╔════╝██╔════╝██╔══██╗    ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██║ ██╔╝██╔════╝
    ██║  ██║██║█████╗  █████╗  █████╗  ██████╔╝    ███████╗   ██║   ███████║██║     █████╔╝ ███████╗
    ██║  ██║██║██╔══╝  ██╔══╝  ██╔══╝  ██╔══██╗    ╚════██║   ██║   ██╔══██║██║     ██╔═██╗ ╚════██║
    ██████╔╝██║██║     ██║     ███████╗██║  ██║    ███████║   ██║   ██║  ██║╚██████╗██║  ██╗███████║
    ╚═════╝ ╚═╝╚═╝     ╚═╝     ╚══════╝╚═╝  ╚═╝    ╚══════╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝
    '''

    def get_differ_stacks(
        self, 
        sieve: dict = None, 
        limit: int = None,
        sort: str = "",
        active_default: bool = True,
        **kwargs
    ):
        """
        Get all differ stacks.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            sort (str): attribute to sort by, default is _id. Add `-` to the beginning of the attribute name to
                        sort in descending order.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this
            pageSize (int: 500): Number of entries to return per page
        Returns:
            pd.DataFrame
        """

        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default

        try:
            depaginated_differ_stacks = self.depaginate(
                "differstacks", sieve, limit=limit, sort=[sort], **kwargs
            )
        except Exception as e:
            raise RuntimeError("Unable to get differ stacks") from e
        else:
            res = pd.DataFrame(depaginated_differ_stacks)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("differ_stack"))

            res.set_index("_id", inplace=True)
            return res


    def get_differ_stack(self, differ_stack_id: str) -> dict:
        """
        Get a single differ stack by its ID.

        Arguments:
            differ_stack_id (str): The ID of the differ stack to retrieve
        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/differstacks/{differ_stack_id}"), 
                headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to get differ stack {differ_stack_id}") from e

        return res.json()

    def post_differ_stack(
        self,
        task_id: str,
        differ_stack: List[Dict]
    ):
        """
        Post a new differ stack to the database.

        Arguments:
            task_id (str)
            differ_stack List[Dict]

        Returns:
            dict

        """

        differ_stack_object = {
            "active": True,
            "task_id": task_id,
            "differ_stack": differ_stack
        }
        res = self._try_request(
            lambda: requests.post(
                self.url("/differstacks"), data=json.dumps(differ_stack_object), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError("Failed to post differ stack") from e
        return res.json()


    """
    █████╗  ██████╗ ███████╗███╗   ██╗████████╗███████╗
    ██╔══██╗██╔════╝ ██╔════╝████╗  ██║╚══██╔══╝██╔════╝
    ███████║██║  ███╗█████╗  ██╔██╗ ██║   ██║   ███████╗
    ██╔══██║██║   ██║██╔══╝  ██║╚██╗██║   ██║   ╚════██║
    ██║  ██║╚██████╔╝███████╗██║ ╚████║   ██║   ███████║
    ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚══════╝
    """

    def post_agent(
            self,
            seg_id: str,
            nucleus_id: str,
            endpoint:tuple,
            merges: dict,
            metadata: dict = {},
            namespace: str = None
        ):
            """
            Post a new task to the database.

            Arguments:
                root_id List(str)
                endpoint tuple(int,int,int)
                hash (str)
                merges dict{str->int}

            Returns:
                dict

            """
            created = utils.date_to_ms()
            agent_task = {
                "active": True,
                "seg_id": seg_id,
                "nucleus_id": nucleus_id,
                "endpoint": endpoint,
                "merges": merges,
                "metadata": metadata,
                "created": created
            }

            if namespace:
                agent_task['namespace'] = namespace 
                
            res = self._try_request(
                lambda: requests.post(
                    self.url("/agents"), data=json.dumps(agent_task), headers=self._headers
                )
            )
            try:
                self._raise_for_status(res)
            except Exception as e:
                raise RuntimeError("Failed to post task") from e
            return res.json()

    def get_agent_job(self, agent_job_id: str) -> dict:
        """
        Get a single agents_job by its ID. 

        Arguments:
            agent_job_id (str): The ID of the agent job to retrieve
        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/agents/{agent_job_id}"), 
                headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to get agent job {agent_job_id}") from e

        return res.json()

    def get_agent_jobs(
        self, 
        sieve: dict = None, 
        limit: int = None, 
        sort: str = "",
        active_default: bool = True,
        **kwargs
    ):
        """
        Get several agent job outputs.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            sort (str): attribute to sort by, default is _id. Add `-` to the beginning of the attribute name to
                        sort in descending order.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this
            pageSize (int: 500): Number of entries to return per page
        Returns:
            pd.DataFrame
        """

        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default

        try:
            depaginated_agent_jobs = self.depaginate(
                "agents", sieve, limit=limit, sort=[sort], **kwargs
            )
        except Exception as e:
            raise RuntimeError("Unable to get agent jobs") from e
        else:
            res = pd.DataFrame(depaginated_agent_jobs)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("agents"))

            res.set_index("_id", inplace=True)
            return res

    def delete_agent(self, agent_job_id: str) -> str:
        """
        Delete a single task.

        Arguments:
            task_id (str): The ID of the task to delete

        Returns:
            dict

        """
        res = self._try_request(
            lambda: requests.delete(
                self.url(f"/agents/{agent_job_id}"), headers=self._headers
            )
        )
        try:
            self._raise_for_status(res)
        except Exception as e:
            raise RuntimeError(f"Unable to delete task {agent_job_id}") from e
        return agent_job_id
