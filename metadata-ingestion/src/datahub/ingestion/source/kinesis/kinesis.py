"""Phase 1 placeholder for the Kinesis source.

The full SDK V2 implementation is added in Phase 2 (see _PLANNING.md). This
stub exists so `setup.py` entry-point registration resolves to a real class
while Phase 1 is in flight.
"""

from typing import Iterable

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kinesis.kinesis_config import KinesisSourceConfig
from datahub.ingestion.source.kinesis.kinesis_report import KinesisSourceReport


class KinesisSource(Source):
    config: KinesisSourceConfig
    report: KinesisSourceReport

    def __init__(self, config: KinesisSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(ctx)
        self.config = config
        self.report = KinesisSourceReport()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "KinesisSource":
        config = KinesisSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        raise NotImplementedError(
            "KinesisSource is not yet implemented — Phase 2 of the Kinesis "
            "connector plan adds metadata extraction."
        )

    def get_report(self) -> SourceReport:
        return self.report
