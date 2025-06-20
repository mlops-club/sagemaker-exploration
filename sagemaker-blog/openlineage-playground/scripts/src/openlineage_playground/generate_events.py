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

# If you want to log to a file instead of Marquez
# from openlineage.client import OpenLineageClient
# from openlineage.client.transport.file import FileConfig, FileTransport
# 
# file_config = FileConfig(
#     log_file_path="ol.json",
#     append=True,
# )
# 
# client = OpenLineageClient(transport=FileTransport(file_config))


# generates job facet
def job(job_name, sql, location):
    facets = {"sql": sql_job.SQLJobFacet(query=sql)}
    if location != None:
        facets.update(
            {
                "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
                    "git", location
                )
            }
        )
    return Job(namespace=namespace, name=job_name, facets=facets)


# generates run racet
def run(run_id, hour):
    return Run(
        runId=run_id,
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(
                nominalStartTime=f"2022-04-14T{twoDigits(hour)}:12:00Z",
                # nominalEndTime=None
            )
        },
    )


# generates dataset
def dataset(name, schema=None, ns=namespace):
    if schema == None:
        facets = {}
    else:
        facets = {"schema": schema}
    return Dataset(namespace=ns, name=name, facets=facets)


# generates output dataset
def outputDataset(dataset, stats):
    output_facets = {"stats": stats, "outputStatistics": stats}
    return OutputDataset(dataset.namespace,
                         dataset.name,
                         facets=dataset.facets,
                         outputFacets=output_facets)


# generates input dataset
def inputDataset(dataset, dq):
    input_facets = {
        "dataQuality": dq,
    }
    return InputDataset(dataset.namespace, dataset.name,
                        facets=dataset.facets,
                        inputFacets=input_facets)


def twoDigits(n):
    if n < 10:
        result = f"0{n}"
    elif n < 100:
        result = f"{n}"
    else:
        raise f"error: {n}"
    return result


now = datetime.now(timezone.utc)


# generates run Event
def runEvents(job_name, sql, inputs, outputs, hour, min, location, duration):
    run_id = str(generate_new_uuid())
    myjob = job(job_name, sql, location)
    myrun = run(run_id, hour)
    started_at = now + timedelta(hours=hour, minutes=min, seconds=20 + round(random() * 10))
    ended_at = started_at + timedelta(minutes=duration, seconds=20 + round(random() * 10))
    return (
        RunEvent(
            eventType=RunState.START,
            eventTime=started_at.isoformat(),
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
        RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=ended_at.isoformat(),
            run=myrun,
            job=myjob,
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        ),
    )


# add run event to the events list
def addRunEvents(events, job_name, sql, inputs, outputs, hour, minutes, location=None, duration=2):
    (start, complete) = runEvents(job_name, sql, inputs, outputs, hour, minutes, location, duration)
    events.append(start)
    events.append(complete)


events = []

# create dataset data
for i in range(0, 5):
    user_counts = dataset("tmp_demo.user_counts")
    user_history = dataset(
        "temp_demo.user_history",
        schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name="id", type="BIGINT", description="the user id"
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="email_domain", type="VARCHAR", description="the user id"
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="status", type="BIGINT", description="the user id"
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="created_at",
                    type="DATETIME",
                    description="date and time of creation of the user",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="updated_at",
                    type="DATETIME",
                    description="the last time this row was updated",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="fetch_time_utc",
                    type="DATETIME",
                    description="the time the data was fetched",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="load_filename",
                    type="VARCHAR",
                    description="the original file this data was ingested from",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="load_filerow",
                    type="INT",
                    description="the row number in the original file",
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="load_timestamp",
                    type="DATETIME",
                    description="the time the data was ingested",
                ),
            ]
        ),
        "snowflake://",
    )

    create_user_counts_sql = """CREATE OR REPLACE TABLE TMP_DEMO.USER_COUNTS AS (
            SELECT DATE_TRUNC(DAY, created_at) date, COUNT(id) as user_count
            FROM TMP_DEMO.USER_HISTORY
            GROUP BY date
            )"""

    # location of the source code
    location = "https://github.com/some/airflow/dags/example/user_trends.py"

    # run simulating Airflow DAG with snowflake operator
    addRunEvents(
        events,
        dag_name + ".create_user_counts",
        create_user_counts_sql,
        [user_history],
        [user_counts],
        i,
        11,
        location,
    )


for event in events:
    from openlineage.client.serde import Serde

    print(event)
    print(Serde.to_json(event))
    # time.sleep(1)
    client.emit(event)

