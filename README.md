# py-rcsb_workflow

[![Build Status](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_workflow?branchName=master)](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=21&branchName=master)

## Introduction

RCSB Python workflow entry points for data processing and ETL/ELT operations.

### Installation

Download the library source software from the project repository:

```bash

git clone  --recurse-submodules https://github.com/rcsb/py-rcsb_workflow.git

# or to make sure the submodules are updated --
git submodule update --recursive --init
git submodule update --recursive --remote

```

Optionally, run test suite (Python versions 3.8) using
[setuptools](https://setuptools.readthedocs.io/en/latest/) or
[tox](http://tox.readthedocs.io/en/latest/example/platform.html):

```bash

  pip install -r requirements.txt
  python setup.py test

or simply run:

  tox
```

Installation is via the program [pip](https://pypi.python.org/pypi/pip).  To run tests
from the source tree, the package must be installed in editable mode (i.e. -e):

```bash
pip install -e .
```
