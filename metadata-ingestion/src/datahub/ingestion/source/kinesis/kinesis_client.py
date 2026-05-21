from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional

from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig

if TYPE_CHECKING:
    from mypy_boto3_kinesis import KinesisClient as Boto3KinesisClient


class KinesisClient:
    """boto3 kinesis wrapper exposing the three calls the source needs.

    list_streams and list_tags_for_stream are surfaced as generators so callers
    do not have to know how kinesis paginates (list_streams has a paginator;
    list_tags_for_stream uses an ExclusiveStartTagKey / HasMoreTags loop).
    """

    def __init__(
        self,
        connection: AwsConnectionConfig,
        region_name: Optional[str] = None,
    ) -> None:
        session = connection.get_session()
        self._client: "Boto3KinesisClient" = session.client(
            "kinesis",
            region_name=region_name or connection.aws_region,
            endpoint_url=connection.aws_endpoint_url,
        )

    @property
    def raw(self) -> "Boto3KinesisClient":
        return self._client

    def list_streams(self) -> Iterable[str]:
        for page in self._client.get_paginator("list_streams").paginate():
            yield from page.get("StreamNames", [])

    def describe_stream_summary(self, stream_name: str) -> Dict[str, Any]:
        response = self._client.describe_stream_summary(StreamName=stream_name)
        return response["StreamDescriptionSummary"]

    def list_tags_for_stream(self, stream_name: str) -> Iterable[Dict[str, str]]:
        exclusive_start_tag_key: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"StreamName": stream_name}
            if exclusive_start_tag_key is not None:
                kwargs["ExclusiveStartTagKey"] = exclusive_start_tag_key
            response = self._client.list_tags_for_stream(**kwargs)
            tags = response.get("Tags", [])
            yield from tags
            if not response.get("HasMoreTags") or not tags:
                return
            exclusive_start_tag_key = tags[-1]["Key"]
