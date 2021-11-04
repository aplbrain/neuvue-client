## *Class* `NeuvueQueue`


neuvueclient.NeuvueQueue abstracts the interfaces to interact with NeuvueQueue.

See neuvueclient/__init__.py for more documentation.



## *Function* `__init__(self, url: str, **kwargs) -> None`


Create a new NeuvueQueue client.

### Arguments
> - **url** (`str`: `None`): The qualified location (including protocol) of the server.



## *Function* `url(self, suffix: str = "") -> str`


Construct a FQ URL.

### Arguments
> - **suffix** (`str`: `None`): The endpoint to access.

### Returns
> - **str** (`None`: `None`): The fully qualified URL.



## *Function* `dtype_columns(self, datatype: str) -> List[str]`


Get a list of columns for a datatype.


## *Function* `delete_graph(self, graph_id: str) -> str`


Delete a single graph.

### Arguments
> - **graph_id** (`str`: `None`): The ID of the graph to delete.

### Returns
    str



## *Function* `get_volume(self, volume_id: str) -> dict`


Get a single volume by its ID.

### Arguments
> - **volume_id** (`str`: `None`): The ID of the volume to retrieve

### Returns
    dict



## *Function* `delete_volume(self, volume_id: str) -> str`


Delete a single volume.

### Arguments
> - **volume_id** (`str`: `None`): The ID of the volume to delete

### Returns
    dict



## *Function* `get_question(self, question_id: str) -> dict`


Get a single question by its ID.

### Arguments
> - **question_id** (`str`: `None`): The ID of the question to retrieve

### Returns
    dict



## *Function* `get_next_question(self, assignee: str, namespace: str) -> dict`


Get the next question for a user.

First checks for an open question; then sorts by highest to lowest priority of unopened questions.

### Arguments
> - **assignee** (`str`: `None`): The username of the assignee
> - **namespace** (`str`: `None`): The app for which the question was assigned

### Returns
    dict



## *Function* `delete_question(self, question_id: str) -> str`


Delete a single question.

### Arguments
> - **question_id** (`str`: `None`): The ID of the question to delete

### Returns
    dict



## *Function* `get_node(self, node_id: str, populate_volume: bool = False) -> dict`


Get a single node by its ID.

### Arguments
> - **node_id** (`str`: `None`): The ID of the node to retrieve
> - **populate_volume** (`bool`: `None`): Whether to populate the graph's volume id with the actual volume object.
### Returns
    dict

