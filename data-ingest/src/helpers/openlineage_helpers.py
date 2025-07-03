"""
OpenLineage helper functions for Metaflow flows.
"""

from datetime import datetime, timezone
from typing import List, Optional

from openlineage.client import OpenLineageClient
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
    processing_engine_run,
    schema_dataset,
    sql_job,
)
from openlineage.client.serde import Serde
from openlineage.client.uuid import generate_new_uuid

# OpenLineage configuration
PRODUCER = "https://github.com/OpenLineage/OpenLineage/tree/1.34.0/integration/sagemaker"  # Fixed for DataHub compatibility
NAMESPACE = "nyc_taxi_pipeline"

# from openlineage.client.transport.http import HttpCompression, HttpConfig, HttpTransport

# http_config = HttpConfig(
#     url="http://localhost:8091",
#     endpoint="/openapi/openlineage/api/v1/lineage",
#     verify=False,
#     compression=HttpCompression.GZIP,
# )


class OpenLineageHelper:
    """Helper class for OpenLineage instrumentation."""

    def __init__(self):
        """Initialize OpenLineage client from environment variables."""
        self.client = OpenLineageClient(
            # transport=HttpTransport(config=http_config)
            # url="http://localhost:8091",
            # type="http",
            # endpoint="/openapi/openlineage/api/v1/lineage",
            # compression="gzip",
            # verify=False,
            # timeout=60,  # Set timeout to 60 seconds
        ).from_environment()

    def emit_lineage_event(
        self,
        event_type: RunState,
        job_name: str,
        inputs: Optional[List[Dataset]] = None,
        outputs: Optional[List[Dataset]] = None,
        sql: Optional[str] = None,
    ) -> None:
        """Emit OpenLineage event for the current step."""

        # Create job facets
        job_facets = {}
        if sql:
            job_facets["sql"] = sql_job.SQLJobFacet(query=sql)

        # Create job
        job = Job(namespace=NAMESPACE, name=job_name, facets=job_facets)

        # Create run
        run = Run(
            runId=str(generate_new_uuid()),
            facets={
                "nominalTime": nominal_time_run.NominalTimeRunFacet(
                    nominalStartTime=datetime.now(timezone.utc).isoformat()
                ),
                "processing_engine": processing_engine_run.ProcessingEngineRunFacet(
                    name="sagemaker", version="1.0.0"
                ),
            },
        )

        # Convert Dataset objects to InputDataset/OutputDataset
        input_datasets = []
        if inputs:
            for dataset in inputs:
                input_datasets.append(
                    InputDataset(
                        namespace=dataset.namespace,
                        name=dataset.name,
                        facets=dataset.facets,
                    )
                )

        output_datasets = []
        if outputs:
            for dataset in outputs:
                output_datasets.append(
                    OutputDataset(
                        namespace=dataset.namespace,
                        name=dataset.name,
                        facets=dataset.facets,
                    )
                )

        # Create event
        event = RunEvent(
            eventTime=datetime.now(timezone.utc).isoformat(),
            producer=PRODUCER,
            run=run,
            job=job,
            eventType=event_type,
            inputs=input_datasets or None,
            outputs=output_datasets or None,
        )

        # Print the event in JSON format for debugging
        # from rich import print_json
        # print_json(Serde.to_json(event))
        # Save the event in JSON format for debugging for each event
        # This will append to the file, so you can see all events in one file
        # with open("openlineage_event.json", "a") as f:
        #     f.write(Serde.to_json(event) + "\n\n")

        # Emit event
        self.client.emit(event)
        print(f"Emitted OpenLineage {event_type} event for job: {job_name}")

    def create_dataset(
        self,
        name: str,
        namespace: str = NAMESPACE,
        schema_fields: Optional[List[schema_dataset.SchemaDatasetFacetFields]] = None,
    ) -> Dataset:
        """Create an OpenLineage Dataset."""
        facets = {}
        if schema_fields:
            facets["schema"] = schema_dataset.SchemaDatasetFacet(fields=schema_fields)

        return Dataset(namespace=namespace, name=name, facets=facets or None)


# Convenience function for creating schema fields
def create_schema_field(
    name: str, type_: str, description: str = ""
) -> schema_dataset.SchemaDatasetFacetFields:
    """Create a schema field for dataset facets."""
    return schema_dataset.SchemaDatasetFacetFields(
        name=name, type=type_, description=description
    )
