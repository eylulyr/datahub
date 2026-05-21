from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError, NoRegionError

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kinesis.kinesis import KinesisSource
from datahub.ingestion.source.kinesis.kinesis_client import KinesisClient
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig


@pytest.fixture
def ctx() -> PipelineContext:
    return PipelineContext(run_id="kinesis-test")


@pytest.fixture
def base_config_dict() -> Dict[str, Any]:
    return {
        "connection": {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",
            "aws_region": "us-east-1",
        },
        "platform_instance": "test-instance",
    }


def _make_summary(
    stream_name: str,
    *,
    shard_count: int = 2,
    encryption: str = "NONE",
    key_id: str = "",
    retention: int = 24,
    status: str = "ACTIVE",
) -> Dict[str, Any]:
    summary: Dict[str, Any] = {
        "StreamARN": f"arn:aws:kinesis:us-east-1:123456789012:stream/{stream_name}",
        "StreamStatus": status,
        "RetentionPeriodHours": retention,
        "EncryptionType": encryption,
        "OpenShardCount": shard_count,
    }
    if key_id:
        summary["KeyId"] = key_id
    return summary


def _build_source(
    config_dict: Dict[str, Any],
    ctx: PipelineContext,
    kinesis_client_mock: MagicMock,
) -> KinesisSource:
    """Construct a KinesisSource with KinesisClient patched to a MagicMock."""
    kinesis_client_mock.raw.meta.region_name = "us-east-1"
    with patch(
        "datahub.ingestion.source.kinesis.kinesis.KinesisClient",
        return_value=kinesis_client_mock,
    ):
        return KinesisSource.create(config_dict, ctx)


# -----------------------------------------------------------------------------
# KinesisClient pagination (lives in kinesis_client.py — hand-rolled tag loop
# and standard boto3 paginator for list_streams).
# -----------------------------------------------------------------------------


class TestKinesisClientPagination:
    def test_list_streams_walks_paginator(self) -> None:
        fake_boto3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = iter(
            [
                {"StreamNames": ["alpha", "beta"]},
                {"StreamNames": ["gamma"]},
            ]
        )
        fake_boto3.get_paginator.return_value = paginator

        client = KinesisClient.__new__(KinesisClient)
        client._client = fake_boto3

        assert list(client.list_streams()) == ["alpha", "beta", "gamma"]
        fake_boto3.get_paginator.assert_called_once_with("list_streams")

    def test_list_tags_for_stream_walks_has_more_tags(self) -> None:
        fake_boto3 = MagicMock()
        fake_boto3.list_tags_for_stream.side_effect = [
            {
                "Tags": [
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                ],
                "HasMoreTags": True,
            },
            {"Tags": [{"Key": "c", "Value": "3"}], "HasMoreTags": False},
        ]

        client = KinesisClient.__new__(KinesisClient)
        client._client = fake_boto3

        tags = list(client.list_tags_for_stream("stream-1"))

        assert tags == [
            {"Key": "a", "Value": "1"},
            {"Key": "b", "Value": "2"},
            {"Key": "c", "Value": "3"},
        ]
        assert fake_boto3.list_tags_for_stream.call_count == 2
        # The second call uses the last seen tag key as the exclusive start.
        second_call_kwargs = fake_boto3.list_tags_for_stream.call_args_list[1].kwargs
        assert second_call_kwargs["ExclusiveStartTagKey"] == "b"

    def test_list_tags_stops_when_has_more_false_on_first_page(self) -> None:
        fake_boto3 = MagicMock()
        fake_boto3.list_tags_for_stream.return_value = {
            "Tags": [{"Key": "only", "Value": "v"}],
            "HasMoreTags": False,
        }

        client = KinesisClient.__new__(KinesisClient)
        client._client = fake_boto3

        assert list(client.list_tags_for_stream("stream-1")) == [
            {"Key": "only", "Value": "v"}
        ]
        assert fake_boto3.list_tags_for_stream.call_count == 1


# -----------------------------------------------------------------------------
# Config-level validator (cross-region stale-removal scoping).
# -----------------------------------------------------------------------------


class TestKinesisConfigValidator:
    def test_stateful_with_remove_stale_requires_platform_instance(
        self, base_config_dict: Dict[str, Any]
    ) -> None:
        bad = dict(base_config_dict)
        del bad["platform_instance"]
        bad["stateful_ingestion"] = {
            "enabled": True,
            "remove_stale_metadata": True,
        }

        with pytest.raises(ConfigurationError, match="platform_instance must be set"):
            KinesisSourceConfig.model_validate(bad)

    def test_validator_passes_when_platform_instance_set(
        self, base_config_dict: Dict[str, Any]
    ) -> None:
        ok = dict(base_config_dict)
        ok["stateful_ingestion"] = {
            "enabled": True,
            "remove_stale_metadata": True,
        }
        config = KinesisSourceConfig.model_validate(ok)
        assert config.platform_instance == "test-instance"

    def test_validator_passes_when_remove_stale_disabled(
        self, base_config_dict: Dict[str, Any]
    ) -> None:
        ok = dict(base_config_dict)
        del ok["platform_instance"]
        ok["stateful_ingestion"] = {
            "enabled": True,
            "remove_stale_metadata": False,
        }
        # Should not raise — the validator only fires when remove_stale is on.
        KinesisSourceConfig.model_validate(ok)


# -----------------------------------------------------------------------------
# KinesisSource construction-time guards (region + platform_instance warning).
# -----------------------------------------------------------------------------


class TestKinesisSourceInit:
    def test_raises_when_region_unresolvable(self, ctx: PipelineContext) -> None:
        config = KinesisSourceConfig(
            connection={"aws_access_key_id": "t", "aws_secret_access_key": "t"},
            platform_instance="test-instance",
        )
        mock_client = MagicMock()
        mock_client.raw.meta.region_name = None

        with (
            patch(
                "datahub.ingestion.source.kinesis.kinesis.KinesisClient",
                return_value=mock_client,
            ),
            pytest.raises(ConfigurationError, match="Unable to determine AWS region"),
        ):
            KinesisSource(config, ctx)

    def test_warns_when_platform_instance_unset(
        self, ctx: PipelineContext, base_config_dict: Dict[str, Any]
    ) -> None:
        no_instance = dict(base_config_dict)
        del no_instance["platform_instance"]

        source = _build_source(no_instance, ctx, MagicMock())

        warnings_str = str(source.report.warnings)
        assert "platform_instance is not set" in warnings_str
        assert "us-east-1" in warnings_str

    def test_no_warning_when_platform_instance_set(
        self, ctx: PipelineContext, base_config_dict: Dict[str, Any]
    ) -> None:
        source = _build_source(base_config_dict, ctx, MagicMock())

        assert not any(
            "platform_instance is not set" in str(w) for w in source.report.warnings
        )


# -----------------------------------------------------------------------------
# test_connection error mapping (Phase-2 pushback #1).
# -----------------------------------------------------------------------------


class TestTestConnection:
    @staticmethod
    def _client_error(code: str, message: str = "test message") -> ClientError:
        return ClientError(
            error_response={"Error": {"Code": code, "Message": message}},
            operation_name="ListStreams",
        )

    def test_capable_when_list_streams_succeeds(
        self, base_config_dict: Dict[str, Any]
    ) -> None:
        mock_client = MagicMock()
        mock_client.raw.list_streams.return_value = {"StreamNames": []}

        with patch(
            "datahub.ingestion.source.kinesis.kinesis.KinesisClient",
            return_value=mock_client,
        ):
            report = KinesisSource.test_connection(base_config_dict)

        assert report.basic_connectivity.capable is True

    @pytest.mark.parametrize(
        "error_code,expected_substring",
        [
            ("AccessDenied", "lack kinesis:ListStreams permission"),
            ("AccessDeniedException", "lack kinesis:ListStreams permission"),
            ("UnrecognizedClientException", "not recognized by the target account"),
            ("InvalidSignatureException", "signature is invalid"),
            ("ExpiredTokenException", "session token has expired"),
            ("ThrottlingException", "throttled the test request"),
        ],
    )
    def test_known_client_error_codes_map_to_specific_messages(
        self,
        base_config_dict: Dict[str, Any],
        error_code: str,
        expected_substring: str,
    ) -> None:
        mock_client = MagicMock()
        mock_client.raw.list_streams.side_effect = self._client_error(error_code)

        with patch(
            "datahub.ingestion.source.kinesis.kinesis.KinesisClient",
            return_value=mock_client,
        ):
            report = KinesisSource.test_connection(base_config_dict)

        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert expected_substring in report.basic_connectivity.failure_reason

    def test_unknown_client_error_keeps_code_and_message(
        self, base_config_dict: Dict[str, Any]
    ) -> None:
        mock_client = MagicMock()
        mock_client.raw.list_streams.side_effect = self._client_error(
            "SomeWeirdError", "kaboom"
        )

        with patch(
            "datahub.ingestion.source.kinesis.kinesis.KinesisClient",
            return_value=mock_client,
        ):
            report = KinesisSource.test_connection(base_config_dict)

        assert report.basic_connectivity.failure_reason is not None
        assert "SomeWeirdError" in report.basic_connectivity.failure_reason
        assert "kaboom" in report.basic_connectivity.failure_reason

    def test_botocore_error_reported_separately(
        self, base_config_dict: Dict[str, Any]
    ) -> None:
        mock_client = MagicMock()
        mock_client.raw.list_streams.side_effect = NoRegionError()

        with patch(
            "datahub.ingestion.source.kinesis.kinesis.KinesisClient",
            return_value=mock_client,
        ):
            report = KinesisSource.test_connection(base_config_dict)

        assert report.basic_connectivity.capable is False
        assert report.basic_connectivity.failure_reason is not None
        assert "AWS SDK error" in report.basic_connectivity.failure_reason


# -----------------------------------------------------------------------------
# Filtering + per-stream resilience in get_workunits_internal.
# -----------------------------------------------------------------------------


class TestExtractionFiltering:
    @staticmethod
    def _setup_streams(
        mock_client: MagicMock,
        summaries: Dict[str, Dict[str, Any]],
    ) -> None:
        mock_client.list_streams.return_value = iter(summaries.keys())
        mock_client.describe_stream_summary.side_effect = lambda name: summaries[name]
        mock_client.list_tags_for_stream.side_effect = lambda name: iter([])

    def test_stream_pattern_deny_drops_streams_and_records_them(
        self, ctx: PipelineContext, base_config_dict: Dict[str, Any]
    ) -> None:
        cfg = dict(base_config_dict)
        cfg["stream_pattern"] = {"deny": [".*-skip$"]}

        mock_client = MagicMock()
        self._setup_streams(
            mock_client,
            {
                "keep-me": _make_summary("keep-me"),
                "filter-me-skip": _make_summary("filter-me-skip"),
            },
        )

        source = _build_source(cfg, ctx, mock_client)
        list(source.get_workunits_internal())

        assert source.report.streams_scanned == 1
        assert "filter-me-skip" in list(source.report.streams_filtered)
        mock_client.describe_stream_summary.assert_called_once_with("keep-me")

    def test_per_stream_exception_does_not_abort_run(
        self, ctx: PipelineContext, base_config_dict: Dict[str, Any]
    ) -> None:
        mock_client = MagicMock()
        mock_client.list_streams.return_value = iter(["bad", "good"])

        def describe(name: str) -> Dict[str, Any]:
            if name == "bad":
                raise RuntimeError("kaboom")
            return _make_summary(name)

        mock_client.describe_stream_summary.side_effect = describe
        mock_client.list_tags_for_stream.side_effect = lambda name: iter([])

        source = _build_source(base_config_dict, ctx, mock_client)
        list(source.get_workunits_internal())

        # The good stream was still scanned despite the bad one failing.
        assert source.report.streams_scanned == 1
        # The failure was captured as a warning, not raised.
        assert any(
            "Exception while extracting stream bad" in str(w)
            for w in source.report.warnings
        )
