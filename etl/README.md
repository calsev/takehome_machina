# ETL Pipeline

This directory contains code for building a pipeline for harvesting machine data.

## Assumptions

The implementation here assumes the individual files fit in memory.
Pandas is used for the implementation under this assumption.

The architecture is not very pipeline-like;
I am not a fan of prematurely optimizing or architecting.
I would be inclined to implement this in production as a single-component batch job triggered by a new machine log being created.
Some extenuating circumstances that would necessitate multiple components:

* Cost: At large scale the different phases may be able to run on different instance types
* Runtime: On the sample data the whole process takes ~6 seconds on a laptop,
but if runtimes ever get long enough that checkpointing is prudent,
then this should be reflected in a 'real' pipeline with sub-job state management.

This toy implementation is idempotent in only the most crass way and no provision is made for re-harvesting logs when the schema changes.
The database is largely a red herring because only one table is used and that is only to check for existence.

## Installation

### Docker

The easiest possible way to run is

```shell
make docker-shell
> run-other-commands-in-the-container
> exit
```

This requires Docker and [Docker Compose 2](https://docs.docker.com/compose/install/linux/)

### Python

A Python environment is required to run most targets.
This is created automatically in the container workflow.

The environment can be created on the host machine by running:

```shell
make env-install # Auto-detect Python installation, either system or Pyenv
BASE_PY=/path/to/my/special/python make env-install # Use custom Python
```

The code should be compatible with any Python >= 3.9.
However, the lockfile is for deployment on Python 3.10.
If using a different Python version, the dependencies will likely need to be updated:

```shell
BASE_PY=/path/to/my/special/python make env-update # Use custom Python and update deps
```

## Running

### Harvest

A script to harvest a data directory is provided at `pipeline/harvest_machine_data.py`.
To run on the sample data:

```shell
> make pipe
```

### Viewing

A script to print statistics for a data file is provided at `reader/print_machine_data.py`.

After harvesting the sample data, the statistics can be viewed:

```shell
> make view
```
