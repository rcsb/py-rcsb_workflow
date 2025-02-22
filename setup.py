# File: setup.py
# Date: 1-Oct-2019
#
# Updates:
#  8-Jun-2021 jdw treat requirements.txt dependencies are authoratative, add markdown README.md text
#
import re
from pathlib import Path
from setuptools import find_packages, setup

version = (re.compile(r"""^__version__ *= *['"]([^'"]+)['"]$""", re.MULTILINE)
    .search(Path("rcsb/db/cli/__init__.py").read_text("utf-8")).group(1))
packages = find_packages(exclude=["rcsb.mock-data", "rcsb.workflow.tests*"])
requirements = [
    r for r in Path("requirements.txt").read_text("utf-8").splitlines()
    if not r.startswith("-")  # Strip pip options (e.g. `--extra-index-url`).
]
console_scripts = [
    "exdb_wf_cli=rcsb.workflow.cli.ExDbExec:main",
    "imgs_exec_cli=rcsb.workflow.cli.ImgExec:main",
]

setup(
    name="rcsb.workflow",
    version=version,
    description="RCSB Python data processing and ETL/ELT workflow entry points",
    long_description_content_type="text/markdown",
    long_description=Path("README.md").read_text(encoding="utf-8"),
    author="John Westbrook",
    author_email="john.westbrook@rcsb.org",
    url="https://github.com/rcsb/py-rcsb_workflow",
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
    entry_points={"console_scripts": console_scripts},
    requires_python=">=3.10",
    install_requires=requirements,
    packages=packages,
    test_suite="rcsb.workflow.tests",
    tests_require=["tox"],
    # This setting for namespace package support -
    zip_safe=False,
)
