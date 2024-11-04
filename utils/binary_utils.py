import base64
import zlib
import struct
import numpy as np


def decode_binary(
    text: str, source_format: str, source_compression: bool
) -> np.ndarray:
    """
    Decodes a base64 encoded binary string into a numpy array.
    :param text: Base64 string from mzML file.
    :param source_format: Source format of the binary string (f for float32, d for double64)
    :param source_compression: Source compression flag (True: zlib encoded, False: uncompressed).
    :return: Decoded numpy array.
    """
    if text is not None:
        decoded_bytes = base64.b64decode(text)
        if source_compression:
            decoded_bytes = zlib.decompress(decoded_bytes)

        if source_format == "f":
            arr_len = len(decoded_bytes) // 4
            dtype = np.float32
        elif source_format == "d":
            arr_len = len(decoded_bytes) // 8
            dtype = np.float64
        else:
            raise NotImplementedError(f"{source_format} is not currently supported")

        arr = struct.unpack(source_format * arr_len, decoded_bytes)
        return np.array(arr, dtype=dtype)
    raise ValueError("Invalid base64 string provided")


def encode_binary(arr: np.ndarray, target_format: str, target_compression: bool) -> str:
    """
    Encodes a numpy array into a base64 encoded binary string to place in mzML file.
    :param arr: numpy array to be encoded.
    :param target_format: Source format of the binary string (f for float32, d for double64)
    :param target_compression: Source compression flag (True: zlib encoded, False: uncompressed).
    :return: Base64 encoded binary string.
    """
    if target_format == "f":
        dtype = np.float32
    elif target_format == "d":
        dtype = np.float64
    else:
        raise NotImplementedError(f"{target_format} is not currently supported")

    if arr.dtype != dtype:
        raise ValueError(f"Array dtype must be {dtype}, but got {arr.dtype}")

    binary_data = struct.pack(target_format * len(arr), *arr.tolist())

    if target_compression:
        binary_data = zlib.compress(binary_data)

    encoded_text = base64.b64encode(binary_data).decode("utf-8")
    return encoded_text
