from pathlib import Path
from typing import Any, Dict, List

import pytest
import time_machine
from botocore.stub import Stubber

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.sink.file import write_metadata_file
from datahub.ingestion.source.kinesis.kinesis import KinesisSource
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.testing import mce_helpers

FROZEN_TIME = "2026-05-21 12:00:00"
test_resources_dir = Path(__file__).parent


# Three streams: two production streams that should survive the filter, and
# one *-test stream that should be denied. events-prod is KMS-encrypted to
# exercise that branch in _extract_stream.
STREAM_NAMES = ["orders-prod", "events-prod", "orders-test"]

# EnhancedMonitoring is required by botocore schema validation for
# StreamDescriptionSummary, even though our source ignores it.
_NO_ENHANCED_MONITORING: List[Dict[str, Any]] = []

SUMMARIES: Dict[str, Dict[str, Any]] = {
    "events-prod": {
        "StreamName": "events-prod",
        "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/events-prod",
        "StreamStatus": "ACTIVE",
        "StreamCreationTimestamp": 1700000000.0,
        "RetentionPeriodHours": 72,
        "EncryptionType": "KMS",
        "KeyId": "alias/events-key",
        "OpenShardCount": 8,
        "EnhancedMonitoring": _NO_ENHANCED_MONITORING,
    },
    "orders-prod": {
        "StreamName": "orders-prod",
        "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/orders-prod",
        "StreamStatus": "ACTIVE",
        "StreamCreationTimestamp": 1700000000.0,
        "RetentionPeriodHours": 24,
        "EncryptionType": "NONE",
        "OpenShardCount": 4,
        "EnhancedMonitoring": _NO_ENHANCED_MONITORING,
    },
    # orders-test is filtered out — never described.
}

TAGS: Dict[str, List[Dict[str, str]]] = {
    "events-prod": [
        {"Key": "env", "Value": "prod"},
        {"Key": "team", "Value": "events"},
    ],
    "orders-prod": [
        {"Key": "env", "Value": "prod"},
        {"Key": "team", "Value": "orders"},
    ],
}


def _kinesis_source(platform_instance: str = "acct1-us-east-1") -> KinesisSource:
    config = KinesisSourceConfig.model_validate(
        {
            "connection": {
                "aws_access_key_id": "test",
                "aws_secret_access_key": "test",
                "aws_region": "us-east-1",
            },
            "platform_instance": platform_instance,
            "env": "PROD",
            "stream_pattern": {"deny": [".*-test$"]},
        }
    )
    return KinesisSource(config, PipelineContext(run_id="kinesis-test"))


def _add_canned_responses(stubber: Stubber) -> None:
    """Wire up the boto3 stub for a complete extraction.

    Streams are iterated in sorted order in the source, so describe + tag
    calls fire alphabetically across the surviving (non-filtered) names:
    events-prod, then orders-prod.
    """
    stubber.add_response(
        "list_streams",
        {"StreamNames": STREAM_NAMES, "HasMoreStreams": False},
    )

    for name in sorted(SUMMARIES.keys()):
        stubber.add_response(
            "describe_stream_summary",
            {"StreamDescriptionSummary": SUMMARIES[name]},
            {"StreamName": name},
        )
        stubber.add_response(
            "list_tags_for_stream",
            {"Tags": TAGS.get(name, []), "HasMoreTags": False},
            {"StreamName": name},
        )


@time_machine.travel(FROZEN_TIME, tick=False)
def test_kinesis_ingest(tmp_path: Path, pytestconfig: pytest.Config) -> None:
    source = _kinesis_source()

    with Stubber(source.kinesis_client.raw) as stubber:
        _add_canned_responses(stubber)
        mce_objects = [wu.metadata for wu in source.get_workunits()]
        stubber.assert_no_pending_responses()

    output_path = tmp_path / "kinesis_mces.json"
    write_metadata_file(output_path, mce_objects)

    mce_helpers.check_golden_file(
        pytestconfig,
        output_path=output_path,
        golden_path=test_resources_dir / "kinesis_mces_golden.json",
    )

    # Sanity: only the two production streams were scanned; orders-test was
    # filtered (recorded) and never described.
    assert source.report.streams_scanned == 2
    assert "orders-test" in list(source.report.streams_filtered)


@time_machine.travel(FROZEN_TIME, tick=False)
def test_kinesis_ingest_is_idempotent(tmp_path: Path) -> None:
    """A second source built from the same config + responses produces
    identical output. Guards against accidental nondeterminism (e.g. set
    iteration order leaking into custom_properties)."""
    first = _kinesis_source()
    with Stubber(first.kinesis_client.raw) as stubber:
        _add_canned_responses(stubber)
        first_out = [wu.metadata for wu in first.get_workunits()]

    second = _kinesis_source()
    with Stubber(second.kinesis_client.raw) as stubber:
        _add_canned_responses(stubber)
        second_out = [wu.metadata for wu in second.get_workunits()]

    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    write_metadata_file(first_path, first_out)
    write_metadata_file(second_path, second_out)

    assert first_path.read_bytes() == second_path.read_bytes()
