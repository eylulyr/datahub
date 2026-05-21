import dataclasses
from dataclasses import dataclass

from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.utilities.lossy_collections import LossyList


@dataclass
class KinesisSourceReport(StaleEntityRemovalSourceReport):
    streams_scanned: int = 0
    streams_filtered: LossyList[str] = dataclasses.field(default_factory=LossyList)
    tags_extracted: int = 0

    def report_stream_scanned(self) -> None:
        self.streams_scanned += 1

    def report_stream_filtered(self, stream_name: str) -> None:
        self.streams_filtered.append(stream_name)

    def report_tags_extracted(self, count: int = 1) -> None:
        self.tags_extracted += count
