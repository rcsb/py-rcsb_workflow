# File: setup.py
# Date: 1-Oct-2019
#
# Updates:
#  8-Jun-2021 jdw treat requirements.txt dependencies are authoratative, add markdown README.md text
#
import re

from setuptools import find_packages
from setuptools import setup

packages = []
thisPackage = "rcsb.workflow"

with open("rcsb/workflow/chem/__init__.py", "r") as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]', fd.read(), re.MULTILINE).group(1)


# Load packages from requirements*.txt
with open("requirements.txt", "r") as ifh:
    packagesRequired = [ln.strip() for ln in ifh.readlines()]

with open("README.md", "r") as ifh:
    longDescription = ifh.read()

if not version:
    raise RuntimeError("Cannot find version information")

setup(
    name=thisPackage,
    version=version,
    description="RCSB Python data processing and ETL/ELT workflow entry points",
    long_description_content_type="text/markdown",
    long_description=longDescription,
    author="John Westbrook",
    author_email="john.westbrook@rcsb.org",
    url="https://github.com/rcsb/py-rcsb_workflow",
    #
    license="Apache 2.0",
    classifiers=[
        "Development Status :: 3 - Alpha",
        # 'Development Status :: 5 - Production/Stable',
        "Intended Audience :: Developers",
        "Natural Language :: English",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    # entry_points={"console_scripts": ["cactvs_annotate_mol=rcsb.workflow.cactvsAnnotateMol:main"]},
    #  The following is somewhat flakey --
    # dependency_links=[],
    install_requires=packagesRequired[1:],
    packages=find_packages(exclude=["rcsb.mock-data", "rcsb.workflow.tests", "rcsb.workflow.tests-*", "tests.*"]),
    package_data={
        # If any package contains *.md or *.rst ...  files, include them:
        "": ["*.md", "*.rst", "*.txt", "*.cfg"]
    },
    #
    test_suite="rcsb.workflow.tests",
    tests_require=["tox"],
    #
    # Not configured ...
    extras_require={"dev": ["check-manifest"], "test": ["coverage"]},
    # Added for
    command_options={"build_sphinx": {"project": ("setup.py", thisPackage), "version": ("setup.py", version), "release": ("setup.py", version)}},
    # This setting for namespace package support -
    zip_safe=False,
)
