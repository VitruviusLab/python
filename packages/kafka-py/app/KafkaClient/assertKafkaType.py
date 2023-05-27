from KafkaClient.KafkaDescriptorTypeEnum import KafkaDescriptorTypeEnum


def assertKafkaType(type: str, offset: int) -> None:
    try:
        KafkaDescriptorTypeEnum(type)
    except ValueError:
        raise Exception(f'Unknown type "{type}" at offset {offset}')
