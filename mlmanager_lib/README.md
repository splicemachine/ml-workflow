# ML-Workflow-Lib
### Description
The ML-Workflow-Lib repository houses shared libraries and utilities within the MLManager
stack (mlflow and bobby containers). For example, rather that copying the Job & Handler SQLAlchemy models
(which are used in both of these containers) into the build context of each Docker container,
we would put it into the shared library, which is `pip` installed in both of the containers. This allows
for changes made to these libraries to be made to all of the components that use them.

### Installation
To install this Python library can be installed like any other: with `pip`. 
Note: This is a Python 3 library as Python 2.7 is being discontinued in 2020. Thus,
it is NOT backwards compatible with Python 2.7. It relies on environment variables
specified in the Docker containers to function (e.g. `DB_HOST`, `DB_USER`...) Therefore,
it is recommended that this module is installed inside either one of these containers
(mlflow, bobby) and tested there. 

```
git clone https://github.com/splicemachine/ml-workflow-lib.git
cd ml-workflow-lib
pip3 install .
```

It requires a PAT to install, which is a build argument in both mlflow and bobby images.
