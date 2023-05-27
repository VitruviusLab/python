def printByteArray(input: bytearray | bytes):
    print("B[", " ".join([hex(byte)[2:].zfill(2) for byte in input]), "]")
