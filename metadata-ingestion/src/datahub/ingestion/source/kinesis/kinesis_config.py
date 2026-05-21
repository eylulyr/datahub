from typing import Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern, ConfigurationError
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class KinesisSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    connection: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description=(
            "AWS credentials and region. Supports access keys, named profile, "
            "instance/role credentials, and AWS SSO. See AwsConnectionConfig."
        ),
    )

    region_name: Optional[str] = Field(
        default=None,
        description=(
            "AWS region to ingest from (e.g. 'us-east-1'). If omitted, falls back "
            "to connection.aws_region, then the AWS default credential chain. "
            "Each ingestion run targets exactly one region."
        ),
    )

    stream_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description=(
            "Regex allow/deny on stream names. Applied after listing and before "
            "describe calls so filtered streams do not consume API quota."
        ),
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None

    @model_validator(mode="after")
    def require_platform_instance_when_stale_removal_enabled(
        self,
    ) -> "KinesisSourceConfig":
        sti = self.stateful_ingestion
        if (
            sti is not None
            and sti.enabled
            and sti.remove_stale_metadata
            and not self.platform_instance
        ):
            raise ConfigurationError(
                "platform_instance must be set when stateful_ingestion.enabled and "
                "stateful_ingestion.remove_stale_metadata are both true. Stateful "
                "stale-removal is scoped by platform_instance; reusing the same "
                "value (or leaving it unset) across (account, region) pairs will "
                "cause streams from one region to be soft-deleted when ingestion "
                "runs against another. Set a distinct platform_instance per "
                "(account, region), e.g. 'acct1-us-east-1'."
            )
        return self

    def get_region(self) -> Optional[str]:
        return self.region_name or self.connection.aws_region
