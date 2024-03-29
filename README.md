# ML-Workflow
## The official repo for Splice Machine's ML Manager and Feature Store

[![Docs](https://readthedocs.org/projects/pysplice/badge/?style=flat)](https://pysplice.readthedocs.io/en/latest/)

This repository is the home to all server-side APIs and configuration for running
the MLManager service in Splice Machine's hosted K8s environment.
<br>

To try ML Manager, for free, click [here](https://cloud.splicemachine.io/register?utm_source=mlmanagergithub&utm_medium=header&utm_campaign=sandbox)
<br>

### Structure

ML-Workflow is broken into 6 main components:

* mlflow: The custom mlflow rest API with added Splice Machine functionality:
  * Host of the mlflow UI
  * Default tracking, artifact, and model registry store set to the Splice Machine database
  * sm_mlflow: an override of a number of tracking APIs in mlflow for nit SQLAlchemy bugs and a few enhancements
  * Host of the Job Tracker UI (deprecated)
  * Additional rest endpoints for deploying models to Kubernetes, the Database, Azure and AWS Sagemaker
  * An authentication mechanism using Splice Shiro for authenticating and authorizing users (more to come)

* bobby: The asynchronous job handler. Bobby performs any actual jobs that are sent to mlflow. Bobby has 1 endpoint that is not available external to the cluster, only from the mlflow pod:
  * Deploying models to the database
  * Deploying models to kubernetes
  * Deploying models to azure
  * Deploying models to aws
  * Scheduling model retraining
  * Watching elasticsearch for logs

* feature store: REST api (FastAPI) for the Splice Machine Feature Store. This manages:
  * Creation of feature store objects (features, feature sets, training sets, training views)
  * Deploying feature sets to the database
  * Creating database triggers to manage the point-in-time versioning of feature values
  * Creating training datasets for ML development with point-in-time consistency
  * Enabling discoverability of features/feature sets/training views to users via APIs (and a coming UI)
  * Tracking models deployed which were trained using feature store training sets
  * Scheduling statistics calculations for feature sets/training sets (coming soon)

* mlrunner: The Java code to deploy ML models to the database. When users of Splice Machine deploy models to the database, the engine with which those models run is wrtten here. Natively, it can support 5 model types
  * Spark ML
  * SKLearn
  * H2O (MOJO only)
  * Keras (Single dimensional inputs only currently)
  * (Coming soon) Kubernetes pod deployments (via a REST endpoint)

* shared: A shared directory by docker images that contain useful tools for database/kubernetes work
  * Custom loguru logger
  * SQLAlchemy tables created for metadata tracking
  * Database authenticator
  * custom Kubernetes API
  * Custom database connection utilities
  * A job ledger for asynchronous job handling
  * A mapping of available cloud environments which ml-workflow can run on

* Infrastructure: The YAML files for model deployment, model retrieval, and model retraining (coming soon)

## Running Locally

You can run ml-workflow locally using docker-compose. The docker-comopse  
is set up, but missing a few fields that the user must provide  
in a <code>.env</code> file. There is a [.env.example](https://github.com/splicemachine/ml-workflow/blob/master/.env.example) file you can copy to a .env file and edit accordingly.<br>

The following fields <b>must</b> be provided:
* DB_PASSWORD
* ENVIRONMENT
  * Available values for ENVIRONMENT include:
     * gcp
     * aws
     * azure
     * default

The following fields <b>may</b> be provided (only if you want to deploy models to the relevant endpoints):
* AZURE_USERNAME
* AZURE_PASSWORD
* AWS_ACCESS_KEY_ID
* AWS_SECRET_ACCESS_KEY
* SAGEMAKER_ROLE

### If you are using a local Splice DB instance

After starting the [standalone database](https://www.github.com/splicemachine/spliceengine) you must run sqlshell (`./sqlshell.sh`) and:
* Create an MLMANAGER user <br>`call syscs_util.syscs_create_user('MLMANAGER','admin');`
* grant the mlmanager user access to all existing schemas
  ```
  grant all privileges on schema sys to mlmanager;
  grant all privileges on schema splice to mlmanager;
  grant all privileges on schema SYSIBM to mlmanager;
  create schema featurestore;
  grant all privileges on schema featurestore to mlmanager;
  ```

Then you can run:
```
docker-compose up mlflow bobby feature_store
```
If you want to include jupyter notebooks in your testing, you can run
```
docker-compose up mlflow bobby feature_store jupyter
```

### Build for testing
Update the docker-compose.yaml with a temporary tag for the image name, then run
```
docker-compose build mlflow bobby feature_store
docker-compose up mlflow bobby feature_store
```
If you want to include jupyter notebooks in your testing, you can run
```
docker-compose build mlflow bobby feature_store jupyter 
docker-compose up mlflow bobby feature_store jupyter
```


## Updating and Releasing

The master branch of this repo holds the most recent code. Stable releases are in the [releases](https://github.com/splicemachine/ml-workflow/releases) section.<br>

### Update

Build and test your docker-compose images, then:
* Update the image tag in your commit to <b>one more than currently in master</b> (ie 0.1.26 -> 0.1.27)
* Open a PR
* Once merged, the imgaes will be rebuilt with the updated tag and pushed (this will become automated soon)

### Release

Releasing is the same as updating, except it must also come with a SQL migration script (in the event that any tables were changed). Put that script in the [releases](https://github.com/splicemachine/ml-workflow/tree/master/releases) folder using the same naming convention (mlmanager.<release>.sql)<br>
We want to move from SQL migration scripts to alembic migrations soon.<br>
After the script has been written and tested, cut a release and add necessary comments, and attach the migration SQL

====================================================================

[ML Manager Official Documentation](https://doc.splicemachine.com/mlmanager_using.html)<br>
[Feature Store Official Documentation](https://doc.splicemachine.com/featurestore_architecture.html)<br>
[Full API Documentation](https://pysplice.readthedocs.io/en/latest/)<br>



__If more regions are added you _must_ update the config.json file in the infrastructure directory__
