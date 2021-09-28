<p align=center><img align=center src='colocarpy.svg' width=300 /></p>
<h3 align=center>c o l o c a r p y</h3>
<h6 align=center>a python client for colocard</h6>

# Installation

```shell
pip3 install git+https://github.com/aplbrain/colocarpy.git
```

# Configuration

Create a configuration file in your home directory `~/.colocarpy/.colocarpy` with your bossdb username and password:

```
[CONFIG]
username = <insert username>
password = <insert password>
```

# Usage

First, create a new colocard instance:

```python
import colocarpy

C = colocarpy.Colocard("http://colocard-server/")
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
