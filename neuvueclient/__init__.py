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
from typing import Any, Callable, List, Optional
import http
import ast

import datetime
import json
import warnings
import configparser
import os

import pandas as pd
import requests

from . import utils
from . import version
from . import validator

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
        if "token" in kwargs:
            auth_method = "Inline Arguments"
            self._refresh_token = kwargs["refresh_token"]
            self._access_token = kwargs["access_token"] 
        
        elif ("NEUVUEQUEUE_REFRESH_TOKEN" in os.environ) and ("NEUVUEQUEUE_ACCESS_TOKEN" in os.environ):
            auth_method = "Environment Variables"
            self._refresh_token = os.environ["NEUVUEQUEUE_REFRESH_TOKEN"]
            self._access_token = os.environ["NEUVUEQUEUE_ACCESS_TOKEN"] 
        else:
            auth_method = "Config File"
            try:
                self.config.read(os.path.expanduser("~/.neuvuequeue/neuvuequeue.cfg"))
                self._refresh_token = self.config["CONFIG"]["refresh_token"]

            except:
                print("No tokens found. Please login. \n")
                self.login()

            self._refresh_authorization_token(self._refresh_token)
            self.config.read(os.path.expanduser("~/.neuvuequeue/neuvuequeue.cfg"))

            self._access_token = self.config["CONFIG"]["access_token"]

        print(f"Auth method: {auth_method}")
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
        conn = http.client.HTTPSConnection(self._url)

        payload = "{\"code\":\"" + code + "\",\"code_type\":\"authorization\"}"

        headers = { 'content-type': "application/json" }
        conn.request("POST", "/auth/tokens", payload, headers)

        res = conn.getresponse()
        data = res.read()
        response = data.decode("utf-8")
        response_dict = ast.literal_eval(response)

        self.config['CONFIG'] = {'refresh_token': response_dict["refresh_token"],
                                 'access_token': response_dict["access_token"]}
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
        conn = http.client.HTTPSConnection(self._url)
        payload = "{\"code\":\"" + refresh + "\",\"code_type\":\"refresh\"}"

        headers = { 'content-type': "application/json" }
        conn.request("POST", "/auth/tokens", payload, headers)

        res = conn.getresponse()
        data = res.read()
        response = data.decode("utf-8")
        response_dict = ast.literal_eval(response)
        access_token = response_dict["access_token"]

        self.config["CONFIG"]["access_token"] = access_token
        
        try:
             os.mkdir(os.path.expanduser("~/.neuvuequeue"))
        except OSError:
            pass

        with open(os.path.expanduser("~/.neuvuequeue/neuvuequeue.cfg"), 'w') as configfile:
            self.config.write(configfile)
        
        return access_token
    
    def _set_authorization_token(self):
        self._access_token = self._refresh_authorization_token(self._refresh_token)

    @property
    def _headers(self) -> dict:
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
                "duration",
                "points",
                "status",
                "seg_id",
                "ng_state"
            ]
        }[datatype]

    def _try_request(self, send_req: Callable[[], Any]) -> Any:
        res = send_req()
        if res.status_code == 401:
            self._set_authorization_token()
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
    
    def get_task(self, task_id: str, populate_points: bool = False) -> dict:
        """
        Get a single task by its ID.

        Arguments:
            task_id (str): The ID of the task to retrieve
            populate_points (bool = False): Populate points for the task object.
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
                "status": "open",
            }
        )
        res = self._try_request(
            lambda: requests.get(
                self.url(f"/tasks"), headers=self._headers, params={"q": query}
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
        populate_points: bool = False
    ):
        """
        Get a list of tasks.

        Arguments:
            sieve (dict): See sieve documentation.
            limit (int: None): The maximum number of items to return.
            active_default (bool: True): If `active` is not a key included in sieve, set it to this
            populate_points (bool): Whether to populate the tasks' point ids with their corresponding point object.

        Returns:
            pd.DataFrame

        """
        if sieve is None:
            sieve = {"active": active_default}
        if "active" not in sieve:
            sieve["active"] = active_default
        
        populate = ["points"] if populate_points else None
        
        try:
            depaginated_tasks = self.depaginate("tasks", sieve, populate=populate, limit=limit)
        except Exception as e:
            raise RuntimeError("Unable to get tasks") from e
        else:
            res = pd.DataFrame(depaginated_tasks)

            # If an empty response, then return an empty dataframe:
            if len(res) == 0:
                return pd.DataFrame([], columns=self.dtype_columns("task"))

            res.set_index("_id", inplace=True)
            res.created = pd.to_datetime(res.created, unit="ms")
            res.opened = pd.to_datetime(res.opened, unit="ms")
            res.closed = pd.to_datetime(res.closed, unit="ms")
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
            seg_id (str = None)
            validate (bool = True)

        Returns:
            dict

        """
        if metadata is None:
            metadata = {}

        if not isinstance(priority, int):
            raise ValueError(f"Priority [{priority}] must be an integer.")

        if not isinstance(duration, int):
            raise ValueError(f"Duration [{duration}] must be an integer.")

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
            "duration": duration,
            "author": author,
            "assignee": assignee,
            "namespace": namespace,
            "instructions": instructions,
            "created": utils.date_to_ms(),
            "seg_id": seg_id,
            "ng_state": ng_state,
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
            seg_id (str = None)
            ng_state (str = None)
            validate (bool = True)

        Returns:
            List[dict]

        """
        if metadata is None:
            metadata = {}

        if not isinstance(priority, int):
            raise ValueError(f"Priority [{priority}] must be an integer.")

        if not isinstance(duration, int):
            raise ValueError(f"Duration [{duration}] must be an integer.")

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
                    "duration": duration,
                    "author": author,
                    "assignee": a,
                    "namespace": namespace,
                    "instructions": instructions,
                    "created": created,
                    "seg_id": seg_id,
                    "ng_state": ng_state,
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

    def patch_task(self, task_id: str, **kwargs):
        """
        Patch a single task. Iterates through each argument passed through kwargs and patches each.
        
        Exmaple:
        > patch_task(
            task_id, 
            instructions = {"prompt": 'do a good job'}, 
            status='pending', 
            priority='100'
        )
        
        Arguments:
            task_id (str): The ID of the question to delete
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
            return 

        for key, value in kwargs.items():
            stri = f"/tasks/{task_id}/{key}"
            res = self._try_request( 
                lambda: requests.patch(
                    self.url(stri), 
                    data =json.dumps({key:value}),
                    headers=self._headers)
            )
            try:
                self._raise_for_status(res)
            except Exception as e:
                raise RuntimeError(f"Unable to patch task {task_id}") from e

        return res.json()