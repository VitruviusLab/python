from enum import Enum

class KafkaDescriptorTypeEnum(Enum):
    UUID = 'uuid'
    STRING = 'string'
    INT_8 = 'int8'
    INT_16 = 'int16'
    INT_32 = 'int32'
    INT_64 = 'int64'
    BOOLEAN = 'boolean'
    NULLABLE_STRING = 'n-string'
    COMPACT_NULLABLE_STRING = 'c-n-string'
