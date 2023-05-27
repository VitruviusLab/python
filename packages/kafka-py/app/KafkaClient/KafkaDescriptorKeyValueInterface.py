from typing import TypedDict, List

from KafkaClient.KafkaDescriptorTypeEnum import KafkaDescriptorTypeEnum
from KafkaClient.KafkaDescriptorArrayType import KafkaDescriptorArrayType


class KafkaDescriptorKeyValueInterface(TypedDict):
    key: str
    type: KafkaDescriptorArrayType | KafkaDescriptorTypeEnum
