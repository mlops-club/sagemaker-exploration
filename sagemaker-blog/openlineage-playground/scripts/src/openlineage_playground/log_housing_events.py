#!/usr/bin/env python3

from datetime import datetime, timedelta, timezone
from random import random
from openlineage.client.client import OpenLineageClient, OpenLineageClientOptions
from openlineage.client.event_v2 import (
    Dataset,
    InputDataset,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
    Job,
)
from openlineage.client.facet_v2 import (
    nominal_time_run,
    parent_run,
    sql_job,
    source_code_location_job,
    schema_dataset,
)
from openlineage.client.uuid import generate_new_uuid
from openlineage.client.serde import Serde
import json

# # === CONFIG ===
PRODUCER = "https://github.com/openlineage-user"
NAMESPACE = "house_regression"
FLOW_NAME = "housing_regression_flow"
REPO_URL = "https://github.com/your-org/pipelines/housing_regression_flow.py"
# API_URL = "http://localhost:9000"
# API_KEY = None

# client = OpenLineageClient(
#     url=API_URL,
#     options=OpenLineageClientOptions(api_key=API_KEY),
# )


from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import ApiKeyTokenProvider, HttpConfig, HttpCompression, HttpTransport

http_config = HttpConfig(
  url="http://localhost:9000",
  endpoint="api/v1/lineage",
  timeout=5,
  verify=False,
#   auth=ApiKeyTokenProvider({"apiKey": "f048521b-dfe8-47cd-9c65-0cb07d57591e"}),
  compression=HttpCompression.GZIP,
)

client = OpenLineageClient(transport=HttpTransport(http_config))

now = datetime.now(timezone.utc)
parent_run_id = str(generate_new_uuid())

# === Datasets ===
house_sales = Dataset(NAMESPACE, "house_sales")
location_info = Dataset(NAMESPACE, "location_info")

cleaned_sales = Dataset(
    NAMESPACE, "cleaned_sales",
    facets={
        "schema": schema_dataset.SchemaDatasetFacet(fields=[
            schema_dataset.SchemaDatasetFacetFields("house_id", "INT"),
            schema_dataset.SchemaDatasetFacetFields("price", "FLOAT"),
            schema_dataset.SchemaDatasetFacetFields("sqft", "FLOAT"),
            schema_dataset.SchemaDatasetFacetFields("bedrooms", "INT"),
            schema_dataset.SchemaDatasetFacetFields("bathrooms", "FLOAT"),
            # Add any other fields present in house_sales
        ])
    }
)

enriched_sales = Dataset(
    NAMESPACE, "enriched_sales",
    facets={
        "schema": schema_dataset.SchemaDatasetFacet(fields=[
            schema_dataset.SchemaDatasetFacetFields("house_id", "INT"),
            schema_dataset.SchemaDatasetFacetFields("price", "FLOAT"),
            schema_dataset.SchemaDatasetFacetFields("sqft", "FLOAT"),
            schema_dataset.SchemaDatasetFacetFields("bedrooms", "INT"),
            schema_dataset.SchemaDatasetFacetFields("bathrooms", "FLOAT"),
            schema_dataset.SchemaDatasetFacetFields("zipcode", "STRING"),
            schema_dataset.SchemaDatasetFacetFields("school_rating", "INT"),
            # Add any other fields present in enriched_sales
        ])
    }
)

features_table = Dataset(
    NAMESPACE, "features",
    facets={
        "schema": schema_dataset.SchemaDatasetFacet(fields=[
            schema_dataset.SchemaDatasetFacetFields("sqft", "FLOAT"),
            schema_dataset.SchemaDatasetFacetFields("bedrooms", "INT"),
            schema_dataset.SchemaDatasetFacetFields("bathrooms", "FLOAT"),
            schema_dataset.SchemaDatasetFacetFields("school_rating", "INT"),
            schema_dataset.SchemaDatasetFacetFields("price", "FLOAT"),
        ])
    }
)

trained_model = Dataset(
    NAMESPACE, "trained_model.pkl",
    facets={"schema": schema_dataset.SchemaDatasetFacet(fields=[
        schema_dataset.SchemaDatasetFacetFields("model_type", "STRING"),
        schema_dataset.SchemaDatasetFacetFields("framework", "STRING"),
        schema_dataset.SchemaDatasetFacetFields("version", "STRING")
    ])}
)

# === Parent Flow Job ===
parent_job = Job(
    namespace=NAMESPACE,
    name=FLOW_NAME,
    facets={"sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(
        type="git", url=REPO_URL)}
)

parent_run_obj = Run(
    runId=parent_run_id,
    facets={"nominalTime": nominal_time_run.NominalTimeRunFacet(nominalStartTime=now.isoformat())}
)

# RunEvents are the only thing that ever gets emitted
client.emit(RunEvent(
    eventType=RunState.START,
    eventTime=now.isoformat(),
    run=parent_run_obj,
    job=parent_job,
    producer=PRODUCER
))

# === Steps ===
def emit_step(step_name, inputs=None, outputs=None, sql=None, duration=5):
    job = Job(
        namespace=NAMESPACE,
        name=f"{FLOW_NAME}.{step_name}",
        facets={
            "sql": sql_job.SQLJobFacet(query=sql) if sql else None,
            "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(type="git", url=REPO_URL)
        }
    )

    run_id = str(generate_new_uuid())
    run_obj = Run(
        runId=run_id,
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(now.isoformat()),
            "parent": parent_run.ParentRunFacet(
                run={"runId": parent_run_id},
                job={"namespace": NAMESPACE, "name": FLOW_NAME}
            )
        }
    )

    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(seconds=duration)

    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=start_time.isoformat(),
        run=run_obj,
        job=job,
        producer=PRODUCER,
        inputs=inputs or [],
        outputs=outputs or []
    )

    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=end_time.isoformat(),
        run=run_obj,
        job=job,
        producer=PRODUCER,
        inputs=inputs or [],
        outputs=outputs or []
    )

    for ev in [start_event, complete_event]:
        print(json.dumps(Serde.to_dict(ev), indent=2))
        client.emit(ev)

def emit_sql_job(table_name, input_datasets, output_dataset, sql, parent_job_name, parent_run_id, duration=2):
    job = Job(
        namespace=NAMESPACE,
        name=f"execute_sql.{table_name}",
        facets={
            "sql": sql_job.SQLJobFacet(query=sql),
            "sourceCodeLocation": source_code_location_job.SourceCodeLocationJobFacet(type="git", url=REPO_URL)
        }
    )
    run_id = str(generate_new_uuid())
    run_obj = Run(
        runId=run_id,
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(now.isoformat()),
            "parent": parent_run.ParentRunFacet(
                run={"runId": parent_run_id},
                job={"namespace": NAMESPACE, "name": parent_job_name}
            )
        }
    )
    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(seconds=duration)
    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=start_time.isoformat(),
        run=run_obj,
        job=job,
        producer=PRODUCER,
        inputs=input_datasets,
        outputs=[output_dataset]
    )
    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=end_time.isoformat(),
        run=run_obj,
        job=job,
        producer=PRODUCER,
        inputs=input_datasets,
        outputs=[output_dataset]
    )
    for ev in [start_event, complete_event]:
        print(json.dumps(Serde.to_dict(ev), indent=2))
        client.emit(ev)

# === Emit each step ===

# start step (setup)
emit_step("start")

# prepare_data step (with SQL)
prepare_data_run_id = str(generate_new_uuid())
prepare_data_job_name = f"{FLOW_NAME}.prepare_data"
emit_step("prepare_data",
    inputs=[InputDataset(NAMESPACE, "house_sales"), InputDataset(NAMESPACE, "location_info")],
    outputs=[
        OutputDataset(NAMESPACE, "cleaned_sales", facets=cleaned_sales.facets),
        OutputDataset(NAMESPACE, "enriched_sales", facets=enriched_sales.facets),
        OutputDataset(NAMESPACE, "features", facets=features_table.facets)
    ],
    sql="""
    CREATE OR REPLACE TABLE cleaned_sales AS
    SELECT * FROM house_sales WHERE price BETWEEN 10000 AND 1000000;

    CREATE OR REPLACE TABLE enriched_sales AS
    SELECT s.*, l.zipcode, l.school_rating
    FROM cleaned_sales s
    JOIN location_info l ON s.house_id = l.house_id;

    CREATE OR REPLACE TABLE features AS
    SELECT sqft, bedrooms, bathrooms, school_rating, price
    FROM enriched_sales
    WHERE sqft IS NOT NULL
      AND bedrooms IS NOT NULL
      AND bathrooms IS NOT NULL
      AND school_rating IS NOT NULL;
    """
)

# Sub-jobs for each CREATE statement in prepare_data
emit_sql_job(
    table_name="cleaned_sales",
    input_datasets=[InputDataset(NAMESPACE, "house_sales")],
    output_dataset=OutputDataset(NAMESPACE, "cleaned_sales", facets=cleaned_sales.facets),
    sql="""
    CREATE OR REPLACE TABLE cleaned_sales AS
    SELECT * FROM house_sales WHERE price BETWEEN 10000 AND 1000000;
    """,
    parent_job_name=prepare_data_job_name,
    parent_run_id=prepare_data_run_id
)

emit_sql_job(
    table_name="enriched_sales",
    input_datasets=[
        InputDataset(NAMESPACE, "cleaned_sales", facets=cleaned_sales.facets),
        InputDataset(NAMESPACE, "location_info")
    ],
    output_dataset=OutputDataset(NAMESPACE, "enriched_sales", facets=enriched_sales.facets),
    sql="""
    CREATE OR REPLACE TABLE enriched_sales AS
    SELECT s.*, l.zipcode, l.school_rating
    FROM cleaned_sales s
    JOIN location_info l ON s.house_id = l.house_id;
    """,
    parent_job_name=prepare_data_job_name,
    parent_run_id=prepare_data_run_id
)

emit_sql_job(
    table_name="features",
    input_datasets=[
        InputDataset(NAMESPACE, "enriched_sales", facets=enriched_sales.facets)
    ],
    output_dataset=OutputDataset(NAMESPACE, "features", facets=features_table.facets),
    sql="""
    CREATE OR REPLACE TABLE features AS
    SELECT sqft, bedrooms, bathrooms, school_rating, price
    FROM enriched_sales
    WHERE sqft IS NOT NULL
      AND bedrooms IS NOT NULL
      AND bathrooms IS NOT NULL
      AND school_rating IS NOT NULL;
    """,
    parent_job_name=prepare_data_job_name,
    parent_run_id=prepare_data_run_id
)

# train_model step
emit_step("train_model",
    inputs=[InputDataset(NAMESPACE, "features", facets=features_table.facets)],
    outputs=[OutputDataset(NAMESPACE, "trained_model.pkl", facets=trained_model.facets)]
)

# end step
emit_step("end")

# === Finalize parent flow
client.emit(RunEvent(
    eventType=RunState.COMPLETE,
    eventTime=(now + timedelta(minutes=1)).isoformat(),
    run=parent_run_obj,
    job=parent_job,
    producer=PRODUCER
))
