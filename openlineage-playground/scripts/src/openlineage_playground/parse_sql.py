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
from openlineage.client.uuid import generate_new_uuid
from openlineage.client.serde import Serde
import json
import ast

from openlineage.client.facet_v2 import (
    nominal_time_run,
    parent_run as parent_run_facet,
    sql_job,
    source_code_location_job,
    schema_dataset,
)

# # === CONFIG ===
PRODUCER = "https://github.com/openlineage-user"
NAMESPACE = "parse_sql"
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



from openlineage_sql import parse

# from rich import print

from pathlib import Path

import sqlglot

THIS_DIR = Path(__file__).parent

BIG_SQL_FPATH = THIS_DIR / "big.sql"
BIG_SQL_SCRIPT = BIG_SQL_FPATH.read_text()

statements = [stmt.sql() for stmt in sqlglot.parse(BIG_SQL_SCRIPT, read="snowflake")]

parent_run_id = str(generate_new_uuid())
parent_job = Job(namespace=NAMESPACE, name="big_sql_query")
parent_run_obj = Run(runId=parent_run_id)

# Emit parent START event
parent_start_event = RunEvent(
    eventType=RunState.START,
    eventTime=datetime.now(timezone.utc).isoformat(),
    run=parent_run_obj,
    job=parent_job,
    producer=PRODUCER,
)
print(json.dumps(Serde.to_dict(parent_start_event), indent=2))
client.emit(parent_start_event)

# Helper for parent facet (define inline, not as import)
def make_parent_facet(parent_run_id, parent_namespace, parent_name):
    return {
        "parent": {
            "run": {"runId": parent_run_id},
            "job": {"namespace": parent_namespace, "name": parent_name}
        }
    }

for i, stmt in enumerate(statements[:8]):
    sql_meta = parse(sql=[stmt], dialect="snowflake", default_schema="PATTERN_DB.DATA_SCIENCE_STAGE")

    # Extract input and output tables from sql_meta
    input_datasets = [
        InputDataset(
            namespace=NAMESPACE,
            name=tbl.name,
        )
        for tbl in getattr(sql_meta, "in_tables", [])
    ]
    output_datasets = [
        OutputDataset(
            namespace=NAMESPACE,
            name=tbl.name
        )
        for tbl in getattr(sql_meta, "out_tables", [])
    ]

    # Extract column lineage if available
    column_lineage = []
    for cl in getattr(sql_meta, "column_lineage", []):
        descendant = getattr(cl, "descendant", None)
        descendant_name = descendant.name if descendant else None
        sources = [src.name for src in getattr(cl, "lineage", [])]
        column_lineage.append({"descendant": descendant_name, "sources": sources})

    # Set run facets as plain dicts, as in log_housing_events.py
    run_facets = {}
    if column_lineage:
        run_facets["columnLineage"] = {"fields": column_lineage}
    run_facets["parent"] = {
        "run": {"runId": parent_run_id},
        "job": {"namespace": NAMESPACE, "name": "big_sql_query"}
    }

    run_id = str(generate_new_uuid())
    run_obj = Run(
        runId=run_id,
        facets={
            "nominalTime": nominal_time_run.NominalTimeRunFacet(datetime.now(timezone.utc).isoformat()),
            "parent": parent_run_facet.ParentRunFacet(
                run={"runId": parent_run_id},
                job={"namespace": NAMESPACE, "name": "big_sql_query"}
            )
        }
    )

    job = Job(
        namespace=NAMESPACE,
        name=f"script.sql.{i}",
        facets={},
    )
    # Emit START event for sub-job
    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run_obj,
        job=job,
        producer=PRODUCER,
        inputs=input_datasets,
        outputs=output_datasets,
    )
    # print(json.dumps(Serde.to_dict(start_event), indent=2))
    # client.emit(start_event)  # Comment out or remove this line to avoid 422 errors

    # Emit COMPLETE event for sub-job
    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=run_obj,
        job=job,
        producer=PRODUCER,
        inputs=input_datasets,
        outputs=output_datasets,
    )
    # print(json.dumps(Serde.to_dict(complete_event), indent=2))
    client.emit(complete_event)

# Emit parent COMPLETE event
parent_complete_event = RunEvent(
    eventType=RunState.COMPLETE,
    eventTime=datetime.now(timezone.utc).isoformat(),
    run=parent_run_obj,
    job=parent_job,
    producer=PRODUCER,
)
# print(json.dumps(Serde.to_dict(parent_complete_event), indent=2))
client.emit(parent_complete_event)

