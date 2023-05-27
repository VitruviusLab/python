from typing import List, Any
from dataclasses import dataclass

from math import floor

from KafkaClient.KafkaDescriptorObjectType import KafkaDescriptorObjectType
from KafkaClient.KafkaDescriptorArrayType import KafkaDescriptorArrayType
from KafkaClient.KafkaDescriptorTypeEnum import KafkaDescriptorTypeEnum


@dataclass
class Serialiser:
    buffers: List[bytearray]
    descriptor: KafkaDescriptorObjectType

    def __init__(self, descriptor: KafkaDescriptorObjectType):
        self.buffers = []
        self.descriptor = descriptor

    def serialise(self, data: dict) -> bytearray:
        self.buffers = []

        self.encodeObject(data, self.descriptor)

        return bytearray().join(self.buffers)

    def getBuffers(self) -> List[bytearray]:
        return self.buffers

    def encodeObject(self, data: dict, descriptor: KafkaDescriptorObjectType) -> None:
        for property_descriptor in descriptor:
            key: str = property_descriptor["key"]
            type: KafkaDescriptorArrayType | KafkaDescriptorTypeEnum = (
                property_descriptor["type"]
            )

            if isinstance(type, list):
                scoped_value: Any = data[key]

                if len(scoped_value) == 0:
                    raise Exception(f"Expected non-empty array for property {key}")

                self.writeInt32(len(scoped_value))

                for scoped_element in scoped_value:
                    self.encodeObject(scoped_element, type[0])

                continue

            value: Any = data[key]

            self.encodeValue(value, type)

    def encodeValue(self, value: Any, type: KafkaDescriptorTypeEnum) -> None:
        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.UUID:
            self.writeUUID(value)
            return

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.STRING:
            self.writeString(value)
            return

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.INT_8:
            self.writeInt8(value)
            return

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.INT_16:
            self.writeInt16(value)
            return

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.INT_32:
            self.writeInt32(value)
            return

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.INT_64:
            self.writeInt64(value)
            return

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.BOOLEAN:
            self.writeBoolean(value)
            return

        if KafkaDescriptorTypeEnum(type) == KafkaDescriptorTypeEnum.NULLABLE_STRING:
            if value is not None and len(value) == 0:
                raise Exception("Expected non-empty string")

            self.writeString(value)
            return

        if (
            KafkaDescriptorTypeEnum(type)
            == KafkaDescriptorTypeEnum.COMPACT_NULLABLE_STRING
        ):
            if value is not None and len(value) == 0:
                raise Exception("Expected non-empty string")

            self.writeCompactString(value)
            return

        raise Exception(f"Unknown type {type}")

    def writeUUID(self, value: str) -> None:
        buffer: bytearray = bytearray.fromhex(value.replace("-", ""))

        if len(buffer) != 16:
            raise Exception(f"Expected UUID to be 16 bytes, got {len(buffer)}")

        self.buffers.append(buffer)

    def writeString(self, value: str | None) -> None:
        if value is None:
            self.writeInt16(-1)
            return

        buffer: bytearray = bytearray(value, "utf-8")

        if len(buffer) == 0:
            self.writeInt16(-1)
            return

        self.writeInt16(len(buffer))
        self.buffers.append(buffer)

    def writeUInt8(self, value: int) -> None:
        buffer: bytearray = value.to_bytes(1, byteorder="big", signed=False)
        self.buffers.append(buffer)

    def writeInt8(self, value: int) -> None:
        buffer: bytearray = value.to_bytes(1, byteorder="big", signed=True)
        self.buffers.append(buffer)

    def writeInt16(self, value: int) -> None:
        buffer: bytearray = value.to_bytes(2, byteorder="big", signed=True)
        self.buffers.append(buffer)

    def writeInt32(self, value: int) -> None:
        buffer: bytearray = value.to_bytes(4, byteorder="big", signed=True)
        self.buffers.append(buffer)

    def writeInt64(self, value: int) -> None:
        buffer: bytearray = value.to_bytes(8, byteorder="big", signed=True)
        self.buffers.append(buffer)

    def writeBoolean(self, value: bool) -> None:
        buffer: bytearray = bytearray(1)
        buffer[0] = 1 if value else 0
        self.buffers.append(buffer)

    def writeCompactString(self, value: str | None) -> None:
        if value is None:
            self.writeUInt8(0)
            return

        buffer: bytearray = bytearray(value, "utf-8")

        byteLength: int = len(buffer)

        if byteLength == 0:
            self.writeUInt8(0)
            return

        byteSpan = 1 + floor(byteLength / 256)

        if byteSpan > 8:
            raise Exception(f"String too long, got {byteLength} bytes")

        if byteSpan == 1:
            self.writeUInt8(1)
            self.writeInt8(byteLength)

        if byteSpan == 2:
            self.writeUInt8(2)
            self.writeInt16(byteLength)

        if byteSpan > 2 and byteSpan <= 4:
            self.writeUInt8(4)
            self.writeInt32(byteLength)

        if byteSpan > 4 and byteSpan <= 8:
            self.writeUInt8(8)
            self.writeInt64(byteLength)

        self.buffers.append(buffer)
