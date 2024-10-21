import base64
import zlib
import struct
import numpy as np
import pandas as pd
from tqdm import tqdm


def decode_binary(text: str, source_format: str, source_compression: bool) -> np.ndarray:
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

        if source_format == 'f':
            arr_len = len(decoded_bytes) // 4
            dtype = np.float32
        elif source_format == 'd':
            arr_len = len(decoded_bytes) // 8
            dtype = np.float64
        else:
            raise NotImplementedError(f'{source_format} is not currently supported')

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
    if target_format == 'f':
        dtype = np.float32
    elif target_format == 'd':
        dtype = np.float64
    else:
        raise NotImplementedError(f'{target_format} is not currently supported')

    if arr.dtype != dtype:
        raise ValueError(f'Array dtype must be {dtype}, but got {arr.dtype}')

    binary_data = struct.pack(target_format * len(arr), *arr.tolist())

    if target_compression:
        binary_data = zlib.compress(binary_data)

    encoded_text = base64.b64encode(binary_data).decode('utf-8')
    return encoded_text


def export_to_binary(records, output_file):
    mz = []
    intensity = []

    current_index = None
    current_mz_group = []
    current_intensity_group = []

    for record in tqdm(records, desc='Flattening records'):
        index, retention_time, mz_value, intensity_value, ms_level = record

        # If the index changes, store the current group and start a new one
        if index != current_index:
            if current_index is not None:
                mz.append(np.array(current_mz_group))
                intensity.append(np.array(current_intensity_group))

            current_index = index
            current_mz_group = []
            current_intensity_group = []

        # Append the current mz and intensity values
        current_mz_group.append(mz_value)
        current_intensity_group.append(intensity_value)

    # Append the last group if there's any data left
    if current_mz_group:
        mz.append(np.array(current_mz_group))
        intensity.append(np.array(current_intensity_group))

    write_binary_file(output_file, mz, intensity)


def write_binary_file(file_path, mz_values, intensity_values):
    with open(file_path, 'wb') as f:
        # Write header
        f.write(b'\x00' * 512)

        # Keep track of offsets
        mz_offset = f.tell()
        lengths = []

        # Write m/z data type in reserved header
        f.seek(24)
        mz_array = np.array(mz_values[0])
        mz_dtype_char = np.dtype(mz_array.dtype).char.encode()
        f.write(mz_dtype_char)

        # Write m/z values
        f.seek(mz_offset)   # Should be at byte 512

        for mz_list in tqdm(mz_values, desc='Writing m/z binary'):
            mz_array = np.array(mz_list)
            lengths.append(len(mz_array))
            f.write(mz_array.tobytes())

        intensity_offset = f.tell()
        # Write intensity data type in reserved header
        f.seek(25)
        intensity_array = np.array(intensity_values[0])
        intensity_dtype_char = np.dtype(intensity_array.dtype).char.encode()
        f.write(intensity_dtype_char)

        # Write intensity values
        f.seek(intensity_offset)
        for intensity_list in tqdm(intensity_values, desc='Writing intensity binary'):
            intensity_array = np.array(intensity_list)
            lengths.append(len(intensity_array))
            f.write(intensity_array.tobytes())

        # Record lengths section offset
        lengths_offset = f.tell()

        # Write lengths section
        lengths_array = np.array(lengths, dtype=np.int32)
        f.write(lengths_array.tobytes())

        # Go back and write offsets in the reserved 512 bytes
        f.seek(0)
        f.write(struct.pack('Q', mz_offset))
        f.write(struct.pack('Q', intensity_offset))
        f.write(struct.pack('Q', lengths_offset))


def import_from_binary(file_path):
    mz_values, intensity_values = read_binary_file(file_path)
    df = pd.DataFrame({'mz': mz_values, 'int': intensity_values})
    return df


def read_binary_file(file_path):
    with open(file_path, 'rb') as f:
        # Read offsets from the reserved 512 bytes
        mz_offset = struct.unpack('Q', f.read(8))[0]
        intensity_offset = struct.unpack('Q', f.read(8))[0]
        lengths_offset = struct.unpack('Q', f.read(8))[0]

        # Read m/z and intensity data types from header
        f.seek(24)
        mz_dtype_char = f.read(1).decode()
        mz_dtype = np.dtype(mz_dtype_char)

        f.seek(25)
        intensity_dtype_char = f.read(1).decode()
        intensity_dtype = np.dtype(intensity_dtype_char)

        # Read lengths section
        f.seek(lengths_offset)
        lengths = np.frombuffer(f.read(), dtype=np.int32)

        mz_values = []
        intensity_values = []

        # Read m/z values
        pos = mz_offset
        for length in tqdm(lengths[:len(lengths) // 2], desc="Reading m/z binary"):
            array_size = length * mz_dtype.itemsize
            f.seek(pos)
            array_data = f.read(array_size)
            mz_values.append(np.frombuffer(array_data, dtype=mz_dtype))
            pos += array_size

        # Read intensity values
        for length in tqdm(lengths[len(lengths) // 2:], desc="Reading intensity binary"):
            array_size = length * intensity_dtype.itemsize
            f.seek(pos)
            array_data = f.read(array_size)
            intensity_values.append(np.frombuffer(array_data, dtype=intensity_dtype))
            pos += array_size

    return mz_values, intensity_values