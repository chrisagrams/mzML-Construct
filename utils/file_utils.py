import pandas as pd
import numpy as np
import struct
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq


def flatten_records(records) -> pd.DataFrame:
    mz = []
    intensity = []
    ms_levels = []
    retention_times = []

    current_index = None
    current_mz_group = []
    current_intensity_group = []
    current_ms_level = None
    current_retention_time = None

    for record in tqdm(records, desc="Flattening records"):
        index, retention_time, mz_value, intensity_value, ms_level = record

        # If the index changes, store the current group and start a new one
        if index != current_index:
            if current_index is not None:
                mz.append(np.array(current_mz_group))
                intensity.append(np.array(current_intensity_group))
                ms_levels.append(current_ms_level)
                retention_times.append(current_retention_time)

            current_index = index
            current_mz_group = []
            current_intensity_group = []
            current_ms_level = ms_level
            current_retention_time = retention_time

        # Append the current mz and intensity values
        current_mz_group.append(mz_value)
        current_intensity_group.append(intensity_value)

    # Append the last group if there's any data left
    if current_mz_group:
        mz.append(np.array(current_mz_group))
        intensity.append(np.array(current_intensity_group))
        ms_levels.append(current_ms_level)
        retention_times.append(current_retention_time)

    return pd.DataFrame(
        {
            "mz": mz,
            "int": intensity,
            "ms_level": ms_levels,
            "retention_time": retention_times,
        }
    )


def export_to_binary(records, output_file):
    df = flatten_records(records)
    mz = df["mz"].to_list()
    intensity = df["int"].to_list()
    write_binary_file(output_file, mz, intensity)


def export_to_npy(records, output_file):
    df = flatten_records(records)
    np.save(output_file, df)


def export_to_parquet(records, output_path, compression=None):
    schema = pa.schema(
        [
            ("spec_no", pa.int32()),
            ("ret_time", pa.float64()),
            ("mz", pa.float64()),
            ("int", pa.float64()),
            ("ms_level", pa.int32()),
        ]
    )

    spec_nos, ret_times, mzs, intensities, ms_levels = zip(*records)
    table = pa.Table.from_pydict(
        {
            "spec_no": spec_nos,
            "ret_time": ret_times,
            "mz": mzs,
            "int": intensities,
            "ms_level": ms_levels,
        },
        schema=schema,
    )

    pq.write_table(table, output_path, compression=compression)


def import_from_binary(file_path):
    mz_values, intensity_values = read_binary_file(file_path)
    df = pd.DataFrame({"mz": mz_values, "int": intensity_values})
    return df


def import_from_npy(file_path):
    df = pd.DataFrame(
        np.load(file_path, allow_pickle=True),
        columns=["mz", "int", "ms_level", "retention_time"],
    )
    return df


def import_from_parquet(parquet_file):
    df = pd.read_parquet(parquet_file)

    consolidated_df = (
        df.groupby(["spec_no", "ret_time", "ms_level"])
        .agg({"mz": list, "int": list})
        .reset_index()
    )

    return consolidated_df


def write_binary_file(file_path, mz_values, intensity_values):
    with open(file_path, "wb") as f:
        # Write header
        f.write(b"\x00" * 512)

        # Keep track of offsets
        mz_offset = f.tell()
        lengths = []

        # Write m/z data type in reserved header
        f.seek(24)
        mz_array = np.array(mz_values[0])
        mz_dtype_char = np.dtype(mz_array.dtype).char.encode()
        f.write(mz_dtype_char)

        # Write m/z values
        f.seek(mz_offset)  # Should be at byte 512

        for mz_list in tqdm(mz_values, desc="Writing m/z binary"):
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
        for intensity_list in tqdm(intensity_values, desc="Writing intensity binary"):
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
        f.write(struct.pack("Q", mz_offset))
        f.write(struct.pack("Q", intensity_offset))
        f.write(struct.pack("Q", lengths_offset))


def read_binary_file(file_path):
    with open(file_path, "rb") as f:
        # Read offsets from the reserved 512 bytes
        mz_offset = struct.unpack("Q", f.read(8))[0]
        intensity_offset = struct.unpack("Q", f.read(8))[0]
        lengths_offset = struct.unpack("Q", f.read(8))[0]

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
        for length in tqdm(lengths[: len(lengths) // 2], desc="Reading m/z binary"):
            array_size = length * mz_dtype.itemsize
            f.seek(pos)
            array_data = f.read(array_size)
            mz_values.append(np.frombuffer(array_data, dtype=mz_dtype))
            pos += array_size

        # Read intensity values
        for length in tqdm(
            lengths[len(lengths) // 2 :], desc="Reading intensity binary"
        ):
            array_size = length * intensity_dtype.itemsize
            f.seek(pos)
            array_data = f.read(array_size)
            intensity_values.append(np.frombuffer(array_data, dtype=intensity_dtype))
            pos += array_size

    return mz_values, intensity_values
