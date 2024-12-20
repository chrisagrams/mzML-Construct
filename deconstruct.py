import argparse
import os
from tqdm import tqdm
from lxml import etree
from utils.mzml_utils import *
from utils.binary_utils import decode_binary
from utils.file_utils import export_to_parquet, export_to_binary, export_to_npy


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split an mzML file to XML and binary/parquet/NPY file formats"
    )
    parser.add_argument("input", help="Path to the input mzML file")
    parser.add_argument("output_dir", help="Path to the output directory")
    parser.add_argument(
        "-x",
        "--output_xml",
        help="Path to the output XML file (default: output directory with input file name and .xml extension)",
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=["parquet", "bin", "npy"],
        required=True,
        help="Binary format to export to (parquet, bin, npy)",
    )
    return parser.parse_args()


def change_extension(filepath: str, new_ext: str, output_dir: str) -> str:
    filename = os.path.basename(filepath)
    name, _ = os.path.splitext(filename)
    return os.path.join(output_dir, name + new_ext)


if __name__ == "__main__":
    args = parse_arguments()
    source = args.input
    output_dir = args.output_dir

    output_xml = args.output_xml or change_extension(source, ".xml", output_dir)
    if args.format == "parquet":
        output_binary = change_extension(source, ".parquet", output_dir)
    elif args.format == "bin":
        output_binary = change_extension(source, ".bin", output_dir)
    elif args.format == "npy":
        output_binary = change_extension(source, ".npy", output_dir)

    tree = etree.parse(source)
    root = tree.getroot()
    nsmap = root.nsmap
    namespace = nsmap.get(None)

    records = []

    for spectrum in tqdm(
        root.findall(f".//{{{namespace}}}spectrum"), desc="Processing mzML"
    ):
        index = int(spectrum.get("index"))
        defaultArrayLength = int(spectrum.get("defaultArrayLength"))
        ms_level = get_ms_level(namespace, spectrum)
        retention_time = get_retention_time(namespace, spectrum)
        mz_array = None
        intensity_array = None

        for binaryDataArray in spectrum.findall(f".//{{{namespace}}}binaryDataArray"):
            binaryDataArray.attrib["encodedLength"] = b"0"  # Set encoded length to 0
            binary_type = get_binary_type(namespace, binaryDataArray)
            source_format = get_source_format(namespace, binaryDataArray)
            source_compression = get_source_compression(namespace, binaryDataArray)
            binary = binaryDataArray.find(f".//{{{namespace}}}binary")
            if binary is not None:
                arr = decode_binary(binary.text, source_format, source_compression)
                if binary_type == "m/z array":
                    mz_array = arr
                elif binary_type == "intensity array":
                    intensity_array = arr
                binary.text = ""  # Remove binary from XML

        if mz_array is not None and intensity_array is not None:
            for mz, intensity in zip(mz_array, intensity_array):
                records.append((index, retention_time, mz, intensity, ms_level))

    tree.write(output_xml)

    if args.format == "parquet":
        export_to_parquet(records, output_binary)
    elif args.format == "bin":
        export_to_binary(records, output_binary)
    elif args.format == "npy":
        export_to_npy(records, output_binary)
