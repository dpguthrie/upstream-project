# stdlib
import logging
import json
import os
from typing import Dict

# third party
from dbtc import dbtCloudClient


# Set variables
UPSTREAM_JOB_ID = os.environ.get('UPSTREAM_JOB_ID')
DOWNSTREAM_JOB_ID = os.environ.get('DOWNSTREAM_JOB_ID')
ACCOUNT_ID = os.environ.get('DBT_CLOUD_ACCOUNT_ID')
CAUSE = 'CI run, triggered by github actions'

# Set upstream sources and schema variable
SOURCES = {
    'dbtc': 'upstream_dbtc_schema',
    'yahooquery': 'upstream_yahooquery_schema',
}

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Initialize client class (DBT_CLOUD_SERVICE_TOKEN set as env var)
client = dbtCloudClient()


def trigger_job(job_id: int, payload: Dict, poll_interval: int = 10):
    run = client.cloud.trigger_job_and_poll(ACCOUNT_ID, job_id, payload, poll_interval)
    return run


def get_models_from_metadata(job_id, **kwargs):
    models = client.metadata.get_models(job_id, **kwargs)
    if models['data'] is not None:
        return models['data']['models']
    
    else:
        # Should only be necessary for a job that's never been run
        payload = {
            'cause': 'Metadata not found, triggering simple job with override',
            'steps_override': ['dbt compile']
        }
        run = trigger_job(job_id, payload)
        get_models_from_metadata(job_id, run_id=run['data']['id'])
    
    return []


def get_run_url(run: Dict):
    return 'https://cloud.getdbt.com/#/accounts/{account_id}/projects/{project_id}/runs/{run_id}/'.format(
        **{
            'account_id': run['data']['account_id'],
            'project_id': run['data']['project_id'],
            'run_id': run['data']['id'],
         }
    )


def main():
    # Get pull request #
    github_ref = os.environ.get('GITHUB_REF')
    pull_request = github_ref.split('/')[2]
    schema_override = f'dbt_cloud_pr_{UPSTREAM_JOB_ID}_{pull_request}'
    
    # Trigger upstream project's CI job
    upstream_payload = {
        'cause': CAUSE,
        'git_sha': os.environ.get('GIT_SHA'),
        'github_pull_request_id': int(pull_request),
        'schema_override': schema_override,  
    }

    upstream_run = trigger_job(UPSTREAM_JOB_ID, upstream_payload)
    
    if upstream_run['data']['status'] != 10:
        raise Exception(
            f'Upstream CI run not succesful!  View here - {get_run_url(upstream_run)}'
        )
    
    logging.info(
        f'Upstream CI run finished!  View here - {get_run_url(upstream_run)}'
    )
    
    # Retrieve models from metadata
    upstream_models = get_models_from_metadata(
        UPSTREAM_JOB_ID, run_id=upstream_run['data']['id']
    )
    # Get the names from those upstream models that were recently run
    upstream_models = [
        m['name'] for m in upstream_models if len(m['runResults']) > 0
    ]
    logging.info(f'Upstream models include: {", ".join(upstream_models)}')
    
    # Early exit here if no models have been found
    if len(upstream_models) == 0:
        return
    
    # Retrieve downstream models from metadata
    downstream_models = get_models_from_metadata(DOWNSTREAM_JOB_ID)
    
    # Retrieve names of downstream models that need to be run based on the models
    # they depend on (check if those are included in the upstream_models list)
    downstream_models_to_build = set([
        m['name'] for m in downstream_models
        if any(source.split('.')[-1] in upstream_models for source in m['dependsOn'])
    ])

    logging.info(f'Relevant downstream models include: {", ".join(downstream_models_to_build)}')
    
    # Only trigger downstream job if necessary
    if downstream_models_to_build:
        sources = client.metadata.get_sources(DOWNSTREAM_JOB_ID)['data']['sources']
        sources = list(
            set([s['sourceName'] for s in sources if s['name'] in upstream_models])
        )
        
        variables = {SOURCES[s]: schema_override for s in sources}
    
        # Create the command to override the steps in the downstream job, then trigger
        selectors = ' '.join([name + '+' for name in downstream_models_to_build])
        downstream_payload = {
            'cause': CAUSE,
            'schema_override': schema_override,
            'steps_override': [f"dbt build --vars '{json.dumps(variables)}' --select {selectors}"],
        }

        downstream_run = trigger_job(DOWNSTREAM_JOB_ID, downstream_payload)
        if downstream_run['data']['status'] != 10:
            raise Exception(
                f'Downstream CI run not succesful!  View here - {get_run_url(downstream_run)}'
            )

        logging.info(
            f'Downstream CI run finished!  View here - {get_run_url(downstream_run)}'
        )
        

if __name__ == '__main__':
    main()
