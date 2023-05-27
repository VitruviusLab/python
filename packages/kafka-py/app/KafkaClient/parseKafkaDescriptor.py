from KafkaClient.KafkaDescriptorObjectType import (
    KafkaDescriptorObjectType,
)

from KafkaClient.assertKafkaType import assertKafkaType


def parseKafkaDescriptor(format: str) -> KafkaDescriptorObjectType:
    descriptor: KafkaDescriptorObjectType = []
    offset: int = 0

    while offset < len(format):
        keyDelimiter: int = format.find(":", offset)

        if keyDelimiter == -1:
            raise Exception(f'Missing key delimiter ":" after position {offset}.')

        key: str = format[offset:keyDelimiter]

        if key == "":
            raise Exception(f"Empty key identifier at position {offset}.")

        if not key.isalnum():
            raise Exception(
                f'Invalid key identifier at position {offset}. Found "{key}".'
            )

        # "+1" to skip over the key delimiter
        offset = keyDelimiter + 1

        valueDelimiter: int = format.find("|", offset)

        arrayOpenDelimiter: int = format.find("[", offset)

        # Array
        if arrayOpenDelimiter != -1 and (
            valueDelimiter == -1 or valueDelimiter > arrayOpenDelimiter
        ):
            lookAheadPosition: int = arrayOpenDelimiter + 1
            nestedArrayCount: int = 1

            while nestedArrayCount > 0:
                if lookAheadPosition >= len(format):
                    raise Exception(
                        f"Array starting at position {arrayOpenDelimiter} is not properly closed."
                    )

                character: str = format[lookAheadPosition]

                if character == "]":
                    nestedArrayCount -= 1
                elif character == "[":
                    nestedArrayCount += 1
                # end if

                lookAheadPosition += 1

            # end while

            # "-1" to compensate for the last increment

            arrayCloseDelimiter: int = lookAheadPosition - 1

            # empty array
            if arrayOpenDelimiter + 1 == arrayCloseDelimiter:
                raise Exception(f"Empty array at {arrayOpenDelimiter}")
            # end if

            itemFormat: str = format[arrayOpenDelimiter + 1 : arrayCloseDelimiter]

            itemDescriptor: KafkaDescriptorObjectType = parseKafkaDescriptor(itemFormat)

            descriptor.append({"key": key, "type": [itemDescriptor]})

            # "+1" to skip over the array close delimiter

            offset = arrayCloseDelimiter + 1

            if offset < len(format) and format[offset] == "|":
                offset += 1
            # end if

            continue

        # end if

        # Last value
        if valueDelimiter == -1 and arrayOpenDelimiter == -1:
            subType: str = format[offset:]

            assertKafkaType(subType, offset)

            descriptor.append({"key": key, "type": subType})

            break
        # end if

        # Value
        type: str = format[offset:valueDelimiter]

        assertKafkaType(type, offset)

        descriptor.append({"key": key, "type": type})

        # "+1" to skip over the value delimiter
        offset = valueDelimiter + 1

    # end while

    return descriptor
