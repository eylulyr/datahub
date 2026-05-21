import logging
from typing import Iterable, List, Optional, Union

from botocore.exceptions import BotoCoreError, ClientError

from datahub.configuration.common import ConfigurationError
from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.kinesis.kinesis_client import KinesisClient
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.sdk.container import Container
from datahub.sdk.dataset import Dataset
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

PLATFORM_NAME = "kinesis"
REGION_SUBTYPE = "Region"


class KinesisRegionContainerKey(ContainerKey):
    region: str


@platform_name("Kinesis", id=PLATFORM_NAME)
@config_class(KinesisSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "One regional Container is emitted per ingestion run; every stream is parented to it.",
)
@capability(
    SourceCapability.TAGS,
    "AWS resource tags on each stream are emitted as globalTags using `Key:Value` URN format.",
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Required when ingesting more than one (account, region) pair; "
    "stateful stale-removal is scoped by platform_instance.",
)
@capability(
    SourceCapability.DESCRIPTIONS,
    "A default description is generated per stream; user-supplied descriptions are not "
    "available from the Kinesis API.",
)
@capability(
    SourceCapability.TEST_CONNECTION,
    "Validates AWS credentials and the kinesis:ListStreams permission.",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Detected via stateful_ingestion + remove_stale_metadata. Scope is "
    "(platform_instance, env); cross-region runs must use distinct platform_instance values.",
    supported=True,
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Not supported. Kinesis has no native schema; AWS Glue Schema Registry "
    "integration is out of scope for this connector.",
    supported=False,
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Not supported. Profiling is not meaningful for unbounded streams.",
    supported=False,
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Not supported. Kinesis has no native producer/consumer registry; producer "
    "and consumer lineage should be emitted by the upstream connector "
    "(e.g. Kafka Connect, Flink, Lambda).",
    supported=False,
)
class KinesisSource(StatefulIngestionSourceBase, TestableSource):
    """Extract metadata from AWS Kinesis Data Streams.

    Each ingestion run targets one (account, region) pair: one Region
    Container is emitted, and every allowed stream is emitted as a
    Dataset (subtype Topic) parented to that Container.
    """

    platform: str = PLATFORM_NAME
    config: KinesisSourceConfig
    report: KinesisSourceReport

    def __init__(self, config: KinesisSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config = config
        self.report = KinesisSourceReport()
        self.kinesis_client = KinesisClient(
            connection=config.connection,
            region_name=config.get_region(),
        )

        # Fail fast on unresolvable region — otherwise the first boto3 call
        # raises NoRegionError deep in get_workunits_internal with no clear
        # signal about which knob is missing.
        self.region: str = (
            config.get_region() or self.kinesis_client.raw.meta.region_name or ""
        )
        if not self.region:
            raise ConfigurationError(
                "Unable to determine AWS region. Set `region_name` in the recipe, "
                "`connection.aws_region`, or AWS_DEFAULT_REGION (or ~/.aws/config) "
                "in the environment running the ingestion."
            )

        # Without platform_instance, Dataset URNs are scoped only by
        # (platform, name, env). Stream names commonly repeat across regions,
        # so two regional recipes that omit platform_instance will silently
        # collide on the dataset side (the regional container does stay
        # unique because KinesisRegionContainerKey hashes in `region`).
        # The config validator already errors when stateful + remove_stale
        # are also on; this warning catches the no-stateful case.
        if not config.platform_instance:
            warning_msg = (
                "platform_instance is not set. For multi-region or multi-account "
                f"ingestion this risks Dataset URN collisions (this run targets "
                f"region '{self.region}'). Set a distinct platform_instance per "
                f"(account, region), e.g. 'acct1-{self.region}'."
            )
            logger.warning(warning_msg)
            self.report.report_warning("kinesis-config", warning_msg)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "KinesisSource":
        config = KinesisSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = KinesisSourceConfig.model_validate(config_dict)
            client = KinesisClient(
                connection=config.connection,
                region_name=config.get_region(),
            )
            client.raw.list_streams(Limit=1)
        except ClientError as e:
            error = e.response.get("Error", {}) if hasattr(e, "response") else {}
            code = error.get("Code", "Unknown")
            message = error.get("Message", str(e))
            reasons = {
                "AccessDenied": (
                    "AWS credentials are valid but lack kinesis:ListStreams permission."
                ),
                "AccessDeniedException": (
                    "AWS credentials are valid but lack kinesis:ListStreams permission."
                ),
                "UnrecognizedClientException": (
                    "AWS access key is not recognized by the target account."
                ),
                "InvalidSignatureException": (
                    "AWS request signature is invalid; check the secret key "
                    "and that the client clock is in sync."
                ),
                "ExpiredTokenException": (
                    "AWS session token has expired; refresh credentials."
                ),
                "ThrottlingException": (
                    "Kinesis API throttled the test request; retry."
                ),
            }
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False,
                    failure_reason=reasons.get(code, f"AWS {code}: {message}"),
                ),
            )
        except BotoCoreError as e:
            # Covers NoRegionError, EndpointConnectionError, NoCredentialsError, etc.
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=f"AWS SDK error: {e}"
                ),
            )
        except Exception as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=str(e)
                ),
            )
        return TestConnectionReport(
            basic_connectivity=CapabilityReport(capable=True),
            capability_report={
                SourceCapability.TEST_CONNECTION: CapabilityReport(capable=True),
            },
        )

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        container_key = KinesisRegionContainerKey(
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
            region=self.region,
        )

        yield Container(
            container_key=container_key,
            display_name=self.region,
            subtype=REGION_SUBTYPE,
            description=f"AWS Kinesis streams in region {self.region}",
        )

        # Sort stream names for deterministic ordering — keeps golden files stable.
        for stream_name in sorted(self.kinesis_client.list_streams()):
            if not self.config.stream_pattern.allowed(stream_name):
                self.report.report_stream_filtered(stream_name)
                continue
            try:
                yield from self._extract_stream(stream_name, container_key)
            except Exception as exc:
                logger.exception("Failed to extract Kinesis stream %s", stream_name)
                self.report.report_warning(
                    "stream",
                    f"Exception while extracting stream {stream_name}: {exc}",
                )

    def _extract_stream(
        self,
        stream_name: str,
        container_key: KinesisRegionContainerKey,
    ) -> Iterable[Entity]:
        summary = self.kinesis_client.describe_stream_summary(stream_name)

        custom_properties = {
            "StreamARN": summary["StreamARN"],
            "StreamStatus": summary["StreamStatus"],
            "RetentionPeriodHours": str(summary["RetentionPeriodHours"]),
            "EncryptionType": summary.get("EncryptionType", "NONE"),
            "OpenShardCount": str(summary["OpenShardCount"]),
        }
        if summary.get("EncryptionType") == "KMS" and summary.get("KeyId"):
            custom_properties["KeyId"] = summary["KeyId"]

        aws_tags = list(self.kinesis_client.list_tags_for_stream(stream_name))
        tag_urns = [make_tag_urn(f"{t['Key']}:{t['Value']}") for t in aws_tags]
        if tag_urns:
            self.report.report_tags_extracted(len(tag_urns))

        self.report.report_stream_scanned()

        yield Dataset(
            platform=self.platform,
            name=stream_name,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            subtype=DatasetSubTypes.TOPIC,
            description=f"AWS Kinesis stream {stream_name} in {self.region}",
            custom_properties=custom_properties,
            tags=tag_urns or None,
            parent_container=container_key,
        )

    def get_report(self) -> SourceReport:
        return self.report
