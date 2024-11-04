import argparse
from lxml import etree
from tqdm import tqdm
import numpy as np
from utils.mzml_utils import *
from utils.binary_utils import encode_binary
from utils.file_utils import import_from_binary, import_from_parquet, import_from_npy


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Construct mzML from XML and binary/parquet files."
    )
    parser.add_argument("xml_file", help="Path to the input XML file")
    parser.add_argument(
        "data_file", help="Path to the input data file (Parquet or binary)"
    )
    parser.add_argument("output_file", help="Path to the output mzML file")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    source = args.xml_file
    source_data = args.data_file
    output_file = args.output_file

    # Parse the XML file
    tree = etree.parse(source)
    root = tree.getroot()
    nsmap = root.nsmap
    namespace = nsmap.get(None)

    # Determine the type of the data file and import accordingly
    if source_data.endswith(".parquet"):
        df = import_from_parquet(source_data)
    elif source_data.endswith(".bin"):
        df = import_from_binary(source_data)
    elif source_data.endswith(".npy"):
        df = import_from_npy(source_data)
    else:
        raise ValueError(
            "The data file must be either a Parquet (.parquet), NPY (.npy), or binary (.bin) file"
        )

    # Process each spectrum in the XML
    for spectrum in tqdm(
        root.findall(f".//{{{namespace}}}spectrum"), desc="Constructing mzML"
    ):
        index = int(spectrum.get("index"))
        spectrum.set("defaultArrayLength", str(len(df["mz"][index])))

        for binaryDataArray in spectrum.findall(f".//{{{namespace}}}binaryDataArray"):
            binary_type = get_binary_type(namespace, binaryDataArray)
            source_format = get_source_format(namespace, binaryDataArray)
            source_compression = get_source_compression(namespace, binaryDataArray)
            binary = binaryDataArray.find(f".//{{{namespace}}}binary")
            if binary is not None:
                if binary_type == "m/z array":
                    binary.text = encode_binary(
                        np.array(df["mz"][index]), source_format, source_compression
                    )
                    binaryDataArray.attrib["encodedLength"] = str(len(binary.text))
                elif binary_type == "intensity array":
                    binary.text = encode_binary(
                        np.array(df["int"][index]), source_format, source_compression
                    )
                    binaryDataArray.attrib["encodedLength"] = str(len(binary.text))

    # Write the modified XML tree to the output mzML file
    tree.write(output_file, pretty_print=True, xml_declaration=True, encoding="utf-8")
