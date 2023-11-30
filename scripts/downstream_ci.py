# stdlib
import logging
import os
import sys
import time
from typing import Dict

# third party
import requests
from dbtc import dbtCloudClient

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


JOB_QUERY = """
query Job($jobId: BigInt!, $runId: BigInt, $schema: String) {
  job(id: $jobId, runId: $runId) {
    models(schema: $schema) {
      name
      uniqueId
      database
      access
    }
  }
}
"""

PUBLIC_MODELS_QUERY = """
query Account($accountId: BigInt!, $filter: PublicModelsFilter) {
  account(id: $accountId) {
    publicModels(filter: $filter) {
      uniqueId
      name
      dependentProjects {
        projectId
        defaultEnvironmentId
        dependentModelsCount
      }
    }
  }
}
"""

ENVIRONMENT_QUERY = """
query Lineage($environmentId: BigInt!, $filter: AppliedResourcesFilter!) {
  environment(id: $environmentId) {
    applied {
      lineage(filter: $filter) {
        uniqueId
        name
        publicParentIds
      }
    }
  }
}
"""


def metadata_request(session: requests.Session, query: str, variables: Dict) -> Dict:
    url = "https://metadata.cloud.getdbt.com/beta/graphql"
    payload = {"query": query, "variables": variables}
    response = session.post(url, json=payload)
    response.raise_for_status()
    return response.json()


def post_comment_to_pr(repo, pr_number, comment, token):
    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {"Authorization": f"token {token}"}
    data = {"body": comment}
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()


# Retrieve environment variables
ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID", None)
JOB_ID = os.getenv("JOB_ID", None)
PULL_REQUEST_ID = int(os.getenv("PULL_REQUEST_ID", None))
GIT_SHA = os.getenv("GIT_SHA", None)
SCHEMA_OVERRIDE = f"dbt_cloud_pr_{JOB_ID}_{PULL_REQUEST_ID}"
TOKEN = os.getenv("DBT_CLOUD_SERVICE_TOKEN", None)
REPO = os.getenv("GITHUB_REPOSITORY", None)
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", None)

session = requests.Session()
session.headers = {
    "Authorization": f"Bearer {TOKEN}",
}
client = dbtCloudClient()

run = client.cloud.trigger_job(
    account_id=ACCOUNT_ID,
    job_id=JOB_ID,
    payload={
        "cause": "Cross-Project Slim CI",
        "git_sha": GIT_SHA,
        "schema_override": SCHEMA_OVERRIDE,
        "github_pull_request_id": PULL_REQUEST_ID,
    },
)

# check status
run_status = run.get("data", {}).get("status", None)
if run_status != 10:
    logger.error("Run was unsuccessful.  Downstream jobs will not be triggered.")
    sys.exit(1)

# Retrieve all public models updated by the job
run_id = run["data"]["id"]
variables = {"jobId": JOB_ID, "runId": run_id, "schema": SCHEMA_OVERRIDE}
results = metadata_request(session, JOB_QUERY, variables)
models = results.get("data", {}).get("job", {}).get("models", [])
public_models = [model for model in models if model["access"].strip() == "public"]
if not public_models:
    logger.info(
        "No public models were updated by this job.  Downstream jobs will not be "
        "triggered."
    )
    sys.exit(0)

# Find all projects that depend on the updated models
logger.info("Finding any projects that depend on the models updated during CI.")
unique_ids = [model["uniqueId"] for model in public_models]
variables = {"accountId": ACCOUNT_ID, "filter": {"uniqueIds": unique_ids}}
results = metadata_request(session, PUBLIC_MODELS_QUERY, variables)
logger.info(f"Results: {results}")
models = results.get("data", {}).get("account", {}).get("publicModels", [])
projects = dict()
for model in models:
    for dep_project in model["dependentProjects"]:
        if dep_project["dependentModelsCount"] > 0:
            project_id = dep_project["projectId"]
            logger.info(
                f"Downstream model found from {model['name']} in project {project_id}"
            )
            if project_id not in projects:
                logging.info(f"Project ID {project_id} has dependent models")
                projects[project_id] = {
                    "environment_id": dep_project["defaultEnvironmentId"],
                    "models": [],
                }
            projects[project_id]["models"].append(model["uniqueId"])

if not projects:
    logger.info(
        "Public models found but are not currently being referenced in any downstream "
        "project."
    )
    sys.exit(0)

# Retrieve downstream CI jobs to trigger
logging.info("Finding downstream CI jobs to trigger.")
jobs_dict = {}
for project_id, project_dict in projects.items():
    variables = {
        "environmentId": project_dict["environment_id"],
        "filter": {"types": ["Model", "Snapshot"]},
    }
    results = metadata_request(session, ENVIRONMENT_QUERY, variables)
    lineage = (
        results.get("data", {})
        .get("environment", {})
        .get("applied", {})
        .get("lineage", [])
    )
    nodes_with_public_parents = [
        node
        for node in lineage
        if any(model in node["publicParentIds"] for model in project_dict["models"])
    ]
    step_override = f'dbt build -s {" ".join([node["name"] for node in nodes_with_public_parents])} --vars \'{{ref_schema_override: {SCHEMA_OVERRIDE}}}\''
    jobs = client.cloud.list_jobs(account_id=ACCOUNT_ID, project_id=project_id)
    ci_jobs = [job for job in jobs.get("data", []) if job["job_type"] == "ci"]
    try:
        job_id = ci_jobs[0]["id"]
        jobs_dict[job_id] = step_override
        logging.info(f"Found CI job {job_id} to trigger in project {project_id}.")
    except IndexError:
        logging.info(f"No CI job found for project: {project_id}")
        pass


run_ids = []
for job_id, step_override in jobs_dict.items():
    logging.info(f"Triggering downstream CI job {job_id}")
    run = client.cloud.trigger_job(
        account_id=ACCOUNT_ID,
        job_id=job_id,
        payload={
            "cause": "Running CI from Upstream Project",
            "git_branch": "main",
            "schema_override": SCHEMA_OVERRIDE,
            "steps_override": [step_override],
        },
        should_poll=False,
    )
    run_ids.append(run["data"]["id"])

errors = []
while run_ids:
    for run_id in run_ids[:]:
        run = client.cloud.get_run(account_id=ACCOUNT_ID, run_id=run_id)
        status = run["data"]["status"]
        if status == 10:
            run_ids.remove(run_id)
        elif status in (20, 30):
            errors.append(run)
            run_ids.remove(run_id)
    time.sleep(10)

if errors:
    logger.error("The following downstream jobs were unsuccessful:")
    for run in errors:
        comment = (
            f'Run ID {run["data"]["id"]} failed.  More info here: {run["data"]["href"]}'
        )
        logger.error(comment)
        post_comment_to_pr(REPO, PULL_REQUEST_ID, comment, GITHUB_TOKEN)
    sys.exit(1)

logger.info("All downstream jobs were successful.")
sys.exit(0)
