# Data Pipeline Project

Please do not fork this repository, but use this repository as a template for your refactoring project. Make Pull Requests to your own repository even if you work alone and mark the checkboxes with an x, if you are done with a topic in the pull request message.

## Project for today
The task for today you can find in the [data-pipeline-project.md](data-pipeline-project.md) file.

## Run the data ingestion

## Run the pipeline
'python src/revenue_pipeline.py --sa_path <path_to_GCP_credentials.json> --bucket <GCS-Bucket-Name> --year 2021 --color <taxi-color> --month <number of month>'
## Environment

Use this file to create a new environment for this task.

```bash
pyenv local 3.11.3
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```
