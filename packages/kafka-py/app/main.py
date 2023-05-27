from KafkaClient.parseKafkaDescriptor import parseKafkaDescriptor
from KafkaClient.KafkaDescriptorObjectType import KafkaDescriptorObjectType
from KafkaClient.JSONObjectType import JSONObjectType
from Serialiser import Serialiser
from Deserialiser import Deserialiser
from utils.printByteArray import printByteArray

"""
    HEADER
"""

print("===============HEADER===============")

requestHeader: str = (
    "apiKey:int16|apiVersion:int16|correlationId:int32|clientId:n-string"
)

requestHeaderDescriptor: KafkaDescriptorObjectType = parseKafkaDescriptor(requestHeader)

print(requestHeaderDescriptor)

headerSerialiser: Serialiser = Serialiser(requestHeaderDescriptor)

headerObject: JSONObjectType = {
    "apiKey": 3,
    "apiVersion": 12,
    "correlationId": 123,
    "clientId": "test",
}


serialisedHeaderValue: bytearray = headerSerialiser.serialise(headerObject)
printByteArray(serialisedHeaderValue)

headerDeserialiser: Deserialiser = Deserialiser(requestHeaderDescriptor)

print(headerDeserialiser.deserialise(serialisedHeaderValue))

"""
    REQUEST METADATA
"""

print("===============REQUEST METADATA===============")

requestMetadata: str = "topics:[topicId:uuid|name:c-n-string]|allowAutoTopicCreate:boolean|includeTopicAuthorisedOperations:boolean"

requestMetadataDescriptor: KafkaDescriptorObjectType = parseKafkaDescriptor(
    requestMetadata
)

print(requestMetadataDescriptor)

requestMetadataSerialiser: Serialiser = Serialiser(requestMetadataDescriptor)

requestMetadataObject: JSONObjectType = {
    "topics": [
        {
            "topicId": "df5caabd-0c41-447c-830b-39734a12ac19",
            "name": "test",
        },
        {
            "topicId": "9e6f5da2-7590-42b0-b84d-54f1b7acedd0",
            "name": None,
        },
    ],
    "allowAutoTopicCreate": True,
    "includeTopicAuthorisedOperations": False,
}

serialisedRequestMetadataValue: bytearray = requestMetadataSerialiser.serialise(
    requestMetadataObject
)
printByteArray(serialisedRequestMetadataValue)

requestMetadataDeserialiser: Deserialiser = Deserialiser(requestMetadataDescriptor)

print(requestMetadataDeserialiser.deserialise(serialisedRequestMetadataValue))

"""
    RESPONSE METADATA
"""

print("===============RESPONSE METADATA===============")

responseMetadata: str = "brokers:[nodeId:int32|host:string|port:int32]|topics:[errorCode:int16|name:string|partitions:[errorCode:int16|partitionIndex:int32|leaderId:int32|replicaNodes:int32|isrNodes:int32]]"

responseMetadataDescriptor: KafkaDescriptorObjectType = parseKafkaDescriptor(
    responseMetadata
)

print(responseMetadataDescriptor)

responseMetadataSerialiser: Serialiser = Serialiser(responseMetadataDescriptor)

responseMetadataObject: JSONObjectType = {
    "brokers": [
        {
            "nodeId": 1,
            "host": "localhost",
            "port": 9092,
        },
        {
            "nodeId": 2,
            "host": "localhost",
            "port": 9093,
        },
    ],
    "topics": [
        {
            "errorCode": 0,
            "name": "test",
            "partitions": [
                {
                    "errorCode": 0,
                    "partitionIndex": 0,
                    "leaderId": 1,
                    "replicaNodes": 1,
                    "isrNodes": 1,
                },
                {
                    "errorCode": 0,
                    "partitionIndex": 1,
                    "leaderId": 2,
                    "replicaNodes": 2,
                    "isrNodes": 2,
                },
            ],
        },
        {
            "errorCode": 0,
            "name": "test2",
            "partitions": [
                {
                    "errorCode": 0,
                    "partitionIndex": 0,
                    "leaderId": 1,
                    "replicaNodes": 1,
                    "isrNodes": 1,
                },
            ],
        },
    ],
}

serialisedResponseMetadataValue: bytearray = responseMetadataSerialiser.serialise(
    responseMetadataObject
)
printByteArray(serialisedResponseMetadataValue)

responseMetadataDeserialiser: Deserialiser = Deserialiser(responseMetadataDescriptor)

print(responseMetadataDeserialiser.deserialise(serialisedResponseMetadataValue))

"""
    FULL RESPONSE METADATA
"""

print("===============FULL RESPONSE METADATA===============")

fullResponseMetadata: str = "size:int32|correlationId:int32|brokers:[nodeId:int32|host:string|port:int32]|topics:[errorCode:int16|name:string|partitions:[errorCode:int16|partitionIndex:int32|leaderId:int32|replicaNodes:int32|isrNodes:int32]]"

fullResponseMetadataDescriptor: KafkaDescriptorObjectType = parseKafkaDescriptor(
    fullResponseMetadata
)

print(fullResponseMetadataDescriptor)

fullResponseMetadataSerialiser: Serialiser = Serialiser(fullResponseMetadataDescriptor)

fullResponseMetadataObject: JSONObjectType = {
    "size": 10,
    "correlationId": 123,
    "brokers": [
        {
            "nodeId": 1,
            "host": "localhost",
            "port": 9092,
        },
        {
            "nodeId": 2,
            "host": "localhost",
            "port": 9093,
        },
    ],
    "topics": [
        {
            "errorCode": 0,
            "name": "test",
            "partitions": [
                {
                    "errorCode": 0,
                    "partitionIndex": 0,
                    "leaderId": 1,
                    "replicaNodes": 1,
                    "isrNodes": 1,
                },
                {
                    "errorCode": 0,
                    "partitionIndex": 1,
                    "leaderId": 2,
                    "replicaNodes": 2,
                    "isrNodes": 2,
                },
            ],
        },
        {
            "errorCode": 0,
            "name": "test2",
            "partitions": [
                {
                    "errorCode": 0,
                    "partitionIndex": 0,
                    "leaderId": 1,
                    "replicaNodes": 1,
                    "isrNodes": 1,
                },
            ],
        },
    ],
}

serialisedFullResponseMetadataValue: bytearray = (
    fullResponseMetadataSerialiser.serialise(fullResponseMetadataObject)
)
printByteArray(serialisedFullResponseMetadataValue)

fullResponseMetadataDeserialiser: Deserialiser = Deserialiser(
    fullResponseMetadataDescriptor
)

print(fullResponseMetadataDeserialiser.deserialise(serialisedFullResponseMetadataValue))

"""
===============HEADER===============
[{'key': 'apiKey', 'type': 'int16'}, {'key': 'apiVersion', 'type': 'int16'}, {'key': 'correlationId', 'type': 'int32'}, {'key': 'clientId', 'type': 'n-string'}]
===============REQUEST METADATA===============
[{'key': 'topics', 'type': [[{'key': 'topicId', 'type': 'uuid'}, {'key': 'name', 'type': 'c-n-string'}]]}, {'key': 'allowAutoTopicCreate', 'type': 'boolean'}, {'key': 'includeTopicAuthorisedOperations', 'type': 'boolean'}]
===============RESPONSE METADATA===============
Traceback (most recent call last):
  File "/home/smashing-quasar/development/projects/qlip/kafka-py/app/main.py", line 40, in <module>
    responseMetadataDescriptor: KafkaDescriptorObjectType = parseKafkaDescriptor(
  File "/home/smashing-quasar/development/projects/qlip/kafka-py/app/KafkaClient/parseKafkaDescriptor.py", line 71, in parseKafkaDescriptor
    itemDescriptor: KafkaDescriptorObjectType = parseKafkaDescriptor(itemFormat)
  File "/home/smashing-quasar/development/projects/qlip/kafka-py/app/KafkaClient/parseKafkaDescriptor.py", line 79, in parseKafkaDescriptor
    if format[offset] == "|":
IndexError: string index out of range
"""
