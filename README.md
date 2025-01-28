# py-rcsb_workflow

[![Build Status](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_apis/build/status/rcsb.py-rcsb_workflow?branchName=master)](https://dev.azure.com/rcsb/RCSB%20PDB%20Python%20Projects/_build/latest?definitionId=21&branchName=master)

RCSB Python workflow entry points for data processing and ETL/ELT operations.

## Setup

> [!IMPORTANT]
> These instructions use [uv](https://docs.astral.sh/uv/).
> To install it, run
> ```bash
> uv -V || curl -L -f https://astral.sh/uv/install.sh | sh
> ```
> Unfortunately, `rcsb.workflow` is not yet friendly to uv or modern pip:
> To work around this, I recommend following the instructions below exactly.

### To use the package

Navigate to a directory you want to create a `.venv` in.
Then run

```bash
uv venv --python 3.10 --seed
uv run \
  pip install \
  --extra-index-url https://pypi.anaconda.org/OpenEye/simple \
  --use-deprecated=legacy-resolver \
  rcsb.workflow~=0.47
uv run exdb_exec_cli --help > /dev/null
```

<!------------------------------------------------------------->
<!-- !!!NOTE TO MAINTAINERS!!! SYNCHRONIZE THE VERSION ABOVE -->
<!------------------------------------------------------------->

<b>Explanation:</b>
`uv pip` does not support deprecated pip options, so true pip needs to be installed.
It's installed directly in the runtime (non-dev) environment with the `--seed` option.
This will not work in Python 3.12 or later.

### Cloning

To run tests, you will need to fetch Git submodules and install the package in editable mode (i.e. `pip -e`).

```bash
git clone \
  --recurse-submodules \
  https://github.com/rcsb/py-rcsb_workflow.git
cd py-rcsb_workflow
uv venv --python 3.10
uv run pip install
  --extra-index-url https://pypi.anaconda.org/OpenEye/simple \
  --use-deprecated=legacy-resolver \
  --editable \
  .
uv run pip install setuptools tox
uv run tox
```
