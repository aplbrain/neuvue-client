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

## *Function* `get_task(self, task_id: str, populate_points: bool = False, convert_states_to_json: bool = False) -> dict`


Get a single task by its ID.

### Arguments
> - **task_id** (`str`: `None`): The ID of the task to retrieve
> - **populate_points** (`bool` = `False`): Populate points for the task object.
> - **convert_states_to_json** (`bool` = `False`): Whether to convert a state url to JSON string
### Returns
    dict

## *Function* `get_tasks(self, task_id: str, populate_points: bool = False, convert_states_to_json: bool = False) -> pd.DataFrame`

Get a list of tasks.

### Arguments
> - **sieve** (`dict`): See sieve documentation.
> - **limit** (`int`: `None`): The maximum number of items to return
> - **active_default** (`bool`: `True`): If `active` is not a key included in sieve, set it to this
> - **populate_points** (`bool`): Whether to populate the tasks' point ids with their corresponding point object.
> - **sort** (`str`): attribute to sort by, default is priority 
> - **return_states** (`bool`): whether to populate tasks' ng states
> - **return_metadata** (`bool`): whether to populate tasks' metadata
> - **convert_states_to_json** (`bool`): whether to convert ng_states to json strings
### Returns
    pd.DataFrame
### Sieving Examples
~~~python
from neuvueclient import NeuvueQueue

client = NeuvueQueue("http://localhost:9005")
~~~

#### Sieve by assignee

~~~python
client.get_tasks(
    sieve={"assignee":"admin"},
    return_states=False, 
    return_metadata=False
)
~~~

#### Sieve by creation time

Returns tasks created between January 1 2022 12:00 and February 1, 2022 12:00 

~~~python
client.get_tasks(
    sieve={
        "created": {
            "$lt": datetime.datetime(2022, 2, 1, 12, 0), # before the specified datetime
            "$gt": datetime.datetime(2022, 1, 1, 12, 0) # after the specified datetime
        }
    }, 
    return_states=False, 
    return_metadata=False
)
~~~

#### Sieve by multiple parameters

Returns tasks:

- assigned to user "admin"
- created before January 1, 2022 10:00
- opened after January 1, 2022 10:00

~~~python
client.get_tasks(
    sieve={
        "assignee": "admin",
        "created": {
            "$lt": datetime.datetime(2022, 1, 1, 10, 0) # before the specified datetime
        },
        "opened": {
            "$gt": datetime.datetime(2022, 1, 1, 10, 0) #after the specified datetime
        }
    }, 
    return_states=False, 
    return_metadata=False
)
~~~
