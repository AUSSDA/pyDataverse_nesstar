# pyDataverse_nesstar

Scripts, files and documentation for the pyDataverse NESSTAR data migration with pyDataverse.


## INSTALL

Requirements:

* pyDataverse
* pydantic

```shell
git clone git@github.com:AUSSDA/pyDataverse_nesstar.git
cd pyDataverse_nesstar
pipenv install
```

## RUN

**Set up**

```shell
pipenv shell
export INSTANCE="INSTANCE_NAME"
```

Place .env files in expected location with needed variables set: see `src/settings.py` to find out more.

**Execute Script**

Before you run the script, adapt the data pipeline control flags in `src/nesstar.py`.

```shell
cd src
python -m nesstar
```

## DEVELOPMENT

**Install**

```shell
git clone git@github.com:AUSSDA/pyDataverse_nesstar.git
cd pyDataverse_nesstar
pipenv install --dev
pre-commit install
```
