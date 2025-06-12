#!/usr/bin/env python3
from datetime import datetime, timedelta, timezone
from random import random

from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import (
    nominal_time_run,
    schema_dataset,
    source_code_location_job,
    sql_job,
)
from openlineage.client.uuid import generate_new_uuid

PRODUCER = "https://github.com/openlineage-user"
namespace = "python_client"
dag_name = "user_trends"

# update to your host
url = "http://localhost:9000"
api_key = "1234567890ckcu028rzu5l"  # this api key is the one from the tutorial
# so it must come from the --seed command

client = OpenLineageClient(
    url=url,
    # optional api key in case marquez requires it. When running marquez in
    # your local environment, you usually do not need this.
    options=OpenLineageClientOptions(api_key=api_key),
)



NAMESPACE = "metaflow"

events = []

# A job represents a process that does transforms input datasets into output datasets.
# E.g. a SQL query, a spark job, a Python script, or a step of a Metaflow or Airflow DAG.
# Jobs can have parents, e.g. the WHOLE Airflow DAG or Metaflow flow.
# RunEvents reference Jobs, representing runs of the Job.
# You can't actually client.emit() a Job. You can only emit RunEvents that reference Jobs.
#
# Is this how we'd represent a Metaflow flow?
#
# Job: Metaflow Flow -- TrainFlow
#   Job: TrainFlow.start
#   Job: TrainFlow.prepare_data
#      Job: CREATE TABLE features AS SELECT (...)
#         Inputs: ...
#         Outputs: features
#      Job: ... (for all SQL queries)
#      Job: SELECT * FROM features
#         Inputs: features
#   Job: TrainFlow.train_model
#         Inputs: features
#         Outputs: model
#   Job: TrainFlow.end



metaflow_train_flow_job = Job(
    namespace=NAMESPACE,
    name="TrainFlow",
    facets={
        "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
            type="git",
            url="https://github.com/mlops-club/metaflow-tutorial.git",
            branch="main",
            version="1234567890abcdef1234567890abcdef12345678",
        )
    },
)

# class RunEvent(
#     *,
#     eventTime: str,
#     producer: str = "",
#     run: Run,
#     job: Job,
#     eventType: EventType | None = None,
#     inputs: list[InputDataset] | None = list,
#     outputs: list[OutputDataset] | None = list
# )


# RunEvent
#   inputs: list[InputDataset]
#   outputs: list[OutputDataset]
#   run:
#     runId: uuidV7
#     facets:
#        nominalTime
#   job: 
  

# the run_id for the start and end events needs to be the same... otherwise
# how can Marquez know that the two events describe the START and END lifecycle events of the same run?
run_id = str(generate_new_uuid())

run_start = RunEvent(
    eventTime=datetime.now(timezone.utc).isoformat(),
    producer=PRODUCER,
    run=Run(
        runId=run_id,
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(
                nominalStartTime=datetime.now(timezone.utc).isoformat(),
                # nominalEndTime=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            )
        },
    ),
    job=metaflow_train_flow_job,
    eventType=RunState.START,
)

# make the event look like the flow ran for 5 minutes
run_end = RunEvent(
    eventTime=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
    producer=PRODUCER,
    run=Run(
        runId=run_id,
        facets={},
    ),
    job=metaflow_train_flow_job,
    eventType=RunState.COMPLETE,
)

events.append(run_start)
events.append(run_end)

for e in events:
    client.emit(e)
