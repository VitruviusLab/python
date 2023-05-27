from dataclasses import dataclass
from KafkaClient.KafkaDescriptorObjectType import KafkaDescriptorObjectType
from KafkaClient.KafkaDescriptorArrayType import KafkaDescriptorArrayType
from KafkaClient.KafkaDescriptorTypeEnum import KafkaDescriptorTypeEnum
from KafkaClient.JSONValueType import JSONValueType


@dataclass
class Deserialiser:
    descriptor: KafkaDescriptorObjectType
    offset: int = 0

    def __init__(self, descriptor: KafkaDescriptorObjectType):
        self.descriptor = descriptor

    def deserialise(self, buffer: bytearray) -> KafkaDescriptorObjectType:
        self.offset = 0

        return self.readObject(buffer, self.descriptor)

    def readObject(
        self, buffer: bytearray, descriptor: KafkaDescriptorObjectType
    ) -> KafkaDescriptorObjectType:
        object: KafkaDescriptorObjectType = {}

        try:
            for property_descriptor in descriptor:
                key: str = property_descriptor["key"]
                type: KafkaDescriptorArrayType | KafkaDescriptorTypeEnum = (
                    property_descriptor["type"]
                )

                if isinstance(type, list):
                    array: list = []

                    array_length: int = self.readInt32(buffer)

                    for i in range(array_length):
                        array.append(self.readObject(buffer, type[0]))

                    # end for

                    object[key] = array

                    continue

                # end if

                object[key] = self.readValue(buffer, type)

            # end for
        except Exception as error:
            raise error

        return object

    def readValue(
        self, buffer: bytearray, type: KafkaDescriptorTypeEnum
    ) -> JSONValueType:
        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.UUID:
            return self.readUUID(buffer)

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.STRING:
            return self.readString(buffer)

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.INT_8:
            return self.readInt8(buffer)

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.INT_16:
            return self.readInt16(buffer)

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.INT_32:
            return self.readInt32(buffer)

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.BOOLEAN:
            return self.readBoolean(buffer)

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.NULLABLE_STRING:
            return self.readString(buffer)

        if (
            KafkaDescriptorTypeEnum(type)
            == KafkaDescriptorTypeEnum.COMPACT_NULLABLE_STRING
        ):
            return self.readCompactString(buffer)

        raise Exception(f"Unsupported type {type}")

    def readBoolean(self, buffer: bytearray) -> bool:
        value: int = int.from_bytes(
            buffer[self.offset : self.offset + 1], byteorder="big", signed=False
        )

        if value != 0 and value != 1:
            raise Exception(f"Invalid boolean value {value}")

        self.offset += 1

        return value

    def readUInt8(self, buffer: bytearray) -> int:
        value: int = int.from_bytes(
            buffer[self.offset : self.offset + 1], byteorder="big", signed=False
        )

        self.offset += 1

        return value

    def readInt8(self, buffer: bytearray) -> int:
        value: int = int.from_bytes(
            buffer[self.offset : self.offset + 1], byteorder="big", signed=True
        )

        self.offset += 1

        return value

    def readInt16(self, buffer: bytearray) -> int:
        value: int = int.from_bytes(
            buffer[self.offset : self.offset + 2], byteorder="big", signed=True
        )

        self.offset += 2

        return value

    def readInt32(self, buffer: bytearray) -> int:
        value: int = int.from_bytes(
            buffer[self.offset : self.offset + 4], byteorder="big", signed=True
        )

        self.offset += 4

        return value

    def readInt64(self, buffer: bytearray) -> int:
        value: int = int.from_bytes(
            buffer[self.offset : self.offset + 8], byteorder="big", signed=True
        )

        self.offset += 8

        return value

    def readString(self, buffer: bytearray) -> str | None:
        length: int = self.readInt16(buffer)

        if length == -1:
            return None

        if length == 0:
            return ""

        value: str = buffer[self.offset : self.offset + length].decode("utf-8")

        self.offset += length

        return value

    def readCompactString(self, buffer: bytearray) -> str | None:
        lengthSpanSize: int = self.readInt8(buffer)

        if lengthSpanSize == 0:
            return None

        length: int = int.from_bytes(
            buffer[self.offset : self.offset + lengthSpanSize],
            byteorder="big",
            signed=True,
        )

        self.offset += lengthSpanSize

        if length == 0:
            return ""

        value: str = buffer[self.offset : self.offset + length].decode("utf-8")

        self.offset += length

        return value

    def readUUID(self, buffer: bytearray) -> str:
        rawValue: str = buffer[self.offset : self.offset + 16].hex()

        if len(rawValue) != 32:
            raise Exception(f"Invalid UUID value {rawValue}")

        value = (
            rawValue[0:8]
            + "-"
            + rawValue[8:12]
            + "-"
            + rawValue[12:16]
            + "-"
            + rawValue[16:20]
            + "-"
            + rawValue[20:32]
        )

        self.offset += 16

        return value
