<p align=center><img align=center src='CARP.svg' width=300 /></p>
<h3 align=center>neuvue-client</h3>
<h6 align=center>a python client for neuvuequeue</h6>

# Installation

```shell
pip3 install git+https://github.com/aplbrain/neuvueclient.git
```

# Configuration

Create a configuration file in your home directory `~/.neuvueclient/neuvue-secret.txt` with your Google Oauth token:

```
[CONFIG]
token = <insert token>
```

# Usage

First, create a new neuvuequeue instance:

```python
import neuvueclient as Client

C = Client.NeuvueQueue("http://neuvuequeue-server/")
```

Now you can do all that your coloheart desires:

```python
C.get_questions(sieve={"namespace": "breadcrumbs"})
```

# Implementation Progress

| Object   | Post | Get by ID | List | Delete |
|----------|------|-----------|------|--------|
| Volume   | ✅ | ✅ | ✅ | ✅ |
| Question | ✅ | ✅ | ✅ | ✅ |
| Node     | ⛔ | ✅ | ✅ | ⛔ |
| Graph    | ✅ | ✅ | ✅ | ✅ |
| Point    | ✅ | ✅ | ✅ | ✅ |
| Task     | ✅ | ✅ | ✅ | ✅ |
