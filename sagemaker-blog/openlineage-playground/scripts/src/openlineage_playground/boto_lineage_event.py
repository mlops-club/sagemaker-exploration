# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "boto3",
#     "openlineage-python",
# ]
# ///
import boto3
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict

DOMAIN_ID = "dzd_d4qfrr8oni8w9z"

# Create a minimal OpenLineage RunEvent
run_id = str(uuid.uuid4())
event: Dict[str, Any] = {
    "eventType": "START",
    "eventTime": datetime.now(timezone.utc).isoformat(),
    "run": {"runId": run_id, "facets": {}},
    "job": {"namespace": "example", "name": "boto_lineage_job", "facets": {}},
    "producer": "boto_lineage_event.py",
    "inputs": [],
    "outputs": [],
    "facets": {},
    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
}

# set region us-east-1 for datazone clinet
client = boto3.client(
    "datazone",
    region_name="us-east-1",)

event_bytes = json.dumps(event).encode("utf-8")
response = client.post_lineage_event(
    domainIdentifier=DOMAIN_ID,
    event=event_bytes
)
print("Response:", response)


# Traceback (most recent call last):
#   File "/Users/ericriddoch/repos/sagemaker-exploration/openlineage-playground/scripts/src/openlineage_playground/boto_lineage_event.py", line 25, in <module>
#     response = client.post_lineage_event(
#                ^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/ericriddoch/repos/sagemaker-exploration/openlineage-playground/scripts/.venv/lib/python3.12/site-packages/botocore/client.py", line 598, in _api_call
#     return self._make_api_call(operation_name, kwargs)
#            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/ericriddoch/repos/sagemaker-exploration/openlineage-playground/scripts/.venv/lib/python3.12/site-packages/botocore/context.py", line 123, in wrapper
#     return func(*args, **kwargs)
#            ^^^^^^^^^^^^^^^^^^^^^
#   File "/Users/ericriddoch/repos/sagemaker-exploration/openlineage-playground/scripts/.venv/lib/python3.12/site-packages/botocore/client.py", line 1061, in _make_api_call
#     raise error_class(parsed_response, operation_name)
# botocore.errorfactory.AccessDeniedException: An error occurred (AccessDeniedException) when calling the PostLineageEvent operation: User is not permitted to perform operation: PostLineageEvent