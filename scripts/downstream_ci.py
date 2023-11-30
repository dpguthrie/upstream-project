# stdlib
import logging
import os
import sys
import time

# third party
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

# Retrieve environment variables
ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID", None)
JOB_ID = os.getenv("JOB_ID", None)
PULL_REQUEST_ID = int(os.getenv("PULL_REQUEST_ID", None))
GIT_SHA = os.getenv("GIT_SHA", None)
SCHEMA_OVERRIDE = f"dbt_cloud_pr_{JOB_ID}_{PULL_REQUEST_ID}"

# service token is an env variable
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
variables = {"jobId": JOB_ID, "run_id": run_id, "schema": SCHEMA_OVERRIDE}
results = client.metadata.query(JOB_QUERY, variables=variables)
logger.info(results)
models = results.get("data", {}).get("job", {}).get("models", [])
public_models = [model for model in models if model["access"] == "public"]

if not public_models:
    logger.info(
        "No public models were updated by this job.  Downstream jobs will not be "
        "triggered."
    )
    sys.exit(0)

# Find all projects that depend on the updated models
logger.info("Finding any projects that depend on the models updated during CI.")
unique_ids = [model["uniqueId"] for model in public_models]
variables = {"account_id": ACCOUNT_ID, "filter": {"uniqueIds": unique_ids}}
results = client.metadata.get(PUBLIC_MODELS_QUERY, variables=variables)
models = results.get("account", {}).get("publicModels", [])
projects = dict()
for model in models:
    for project in model["dependentProjects"]:
        if project["dependentModelsCount"] > 0:
            project_id = project["projectId"]
            if project_id not in projects:
                logging.info(f"Project ID {project_id} has dependent models")
                project[project_id] = {
                    "environment_id": model["defaultEnvironmentId"],
                    models: [],
                }
            project[project_id]["models"].append(model["uniqueId"])

if not projects:
    logger.info(
        "Public models found but are not currently being referenced in any downstream "
        "project."
    )
    sys.exit(1)

# Retrieve downstream CI jobs to trigger
logging.info("Finding downstream CI jobs to trigger.")
jobs = {}
for project_id, project_dict in projects.items():
    variables = {
        "environmentId": project_dict["environment_id"],
        "filter": {"types": ["Model", "Snapshot"]},
    }
    results = client.metadata.get(ENVIRONMENT_QUERY, variables=variables)
    lineage = results.get("environment", {}).get("applied", {}).get("lineage", [])
    nodes_with_public_parents = [
        node
        for node in lineage
        if any(model in node["publicParentIds"] for model in project_dict["models"])
    ]
    step_override = f'dbt build -s {" ".join([node["name"] for node in nodes_with_public_parents])} --vars \'{{ref_schema_override: {SCHEMA_OVERRIDE}}}\''
    jobs = client.cloud.list_jobs(account_id=ACCOUNT_ID, project_id=project_id)
    ci_jobs = [job for job in jobs.get("data", []) if job["job_type"] == "CI"]
    try:
        job_id = ci_jobs[0]["id"]
        logging.info(f"Found CI job {job_id} to trigger in project {project_id}.")
    except IndexError:
        job_id = None
    jobs[job_id] = step_override

run_ids = []
for job_id, step_override in jobs.items():
    logging.info(f"Triggering downstream CI job {job_id}")
    run = client.cloud.trigger_job(
        account_id=ACCOUNT_ID,
        job_id=job_id,
        payload={
            "cause": "Running CI from Upstream Project",
            "git_branch": "main",
            "schema_override": SCHEMA_OVERRIDE,
            "step_override": step_override,
            "github_pull_request_id": PULL_REQUEST_ID,
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
        logger.error(
            f'Run ID {run["data"]["id"]} failed.  More info here: {run["data"]["href"]}'
        )
    sys.exit(1)

logger.info("All downstream jobs were successful.")
sys.exit(0)
