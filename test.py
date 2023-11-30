import dbtc

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


client = dbtc.dbtCloudClient(
    service_token="dbtc_Zx23Z4Yxd2jFYV6ixcRRlvXizT7cv_Qq5k2cgtdYF7Tn63Fj8A"
)

variables = {
    "accountId": 43786,
    "filter": {"uniqueIds": ["model.upstream.int_segment__pages"]},
}
results = client.metadata.query(PUBLIC_MODELS_QUERY, variables)
results
