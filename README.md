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

**Workflow**

In general, the workflow for data imports is always as this:

1. Prepare the CSV file
2. Prepare the script
3. Prepare Dataverse installations
  1. Prepare Dataverse
  2. Create Dataverse
4. Test on `local` Dataverse installation
  1. Upload 1 dataset with datafiles
  2. Review: Developer
  3. Publish the dataset with datafiles
  4. Review: Developer
  5. Upload 10 datasets with datafiles
  6. Publish the datasets
  7. Review: Developer
  8. Upload all datasets with datafiles
  9. Publish the datasets
  10. Review: Developer
  11. Review
5. Test on `development` Dataverse installation
  1. Upload 1 dataset with datafiles
  2. Review: Developer + Ingest
  3. Upload all datasets with datafiles
  4. Review: Developer + Ingest
6. Import on `production` Dataverse installation
  1. Upload 1 dataset with datafiles
  2. Review: Developer + Ingest
  3. Upload all datasets with datafiles
  4. Review: Developer + Ingest
  5. Publish all datasets
  6. Review: Developer + Ingest
7. Clean up the Dataverse installations
  1. Delete/Destroy datasets
  2. Delete Dataverses

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
