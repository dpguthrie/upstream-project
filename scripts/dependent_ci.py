# stdlib
import logging
import os
from typing import Dict

# third party
from dbtc import dbtCloudClient


# Set variables
UPSTREAM_JOB_ID = os.environ.get('UPSTREAM_JOB_ID')
DOWNSTREAM_JOB_ID = os.environ.get('DOWNSTREAM_JOB_ID')
ACCOUNT_ID = os.environ.get('DBT_CLOUD_ACCOUNT_ID')

# Payload
PAYLOAD = {
    'cause': 'CI run, triggered by github actions'
}

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


# Initialize client class (DBT_CLOUD_SERVICE_TOKEN set as env var)
client = dbtCloudClient(service_token='dbts__QIqOqsMyYeXzlZUMhpHsmeT2uA_INHEmzuGzb1EvyP_p72J3kKC3FoA==')


def trigger_job(job_id: int, payload: Dict, poll_interval: int = 10):
    run = client.cloud.trigger_job_and_poll(ACCOUNT_ID, job_id, payload, poll_interval)
    return run


def get_models_from_metadata(job_id, **kwargs):
    models = client.metadata.get_models(job_id, **kwargs)
    if models['data'] is not None:
        return models['data']['models']
    
    else:
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
    # Trigger upstream project's CI job
    upstream_run = trigger_job(UPSTREAM_JOB_ID, PAYLOAD)
    
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
    
    # Retrieve downstream models from metadata
    downstream_models = get_models_from_metadata(DOWNSTREAM_JOB_ID)
    
    # Retrieve names of downstream models that need to be run based on the models
    # they depend on (check if those are included in the upstream_models list)
    downstream_models = set([
        m['name'] for m in downstream_models
        if any(source.split('.')[-1] in upstream_models for source in m['dependsOn'])
    ])
    logging.info(f'Relevant downstream models include: {", ".join(downstream_models)}')
    
    # Only trigger downstream job if necessary
    if downstream_models:
    
        # Create the command to override the steps in the downstream job, then trigger
        selectors = ' '.join([name + '+' for name in downstream_models])
        PAYLOAD['steps_override'] = [f'dbt build -s {selectors}']
        downstream_run = trigger_job(DOWNSTREAM_JOB_ID, PAYLOAD)
        if downstream_run['data']['status'] != 10:
            raise Exception(
                f'Downstream CI run not succesful!  View here - {get_run_url(downstream_run)}'
            )

        logging.info(
            f'Downstream CI run finished!  View here - {get_run_url(downstream_run)}'
        )
        

if __name__ == '__main__':
    main()