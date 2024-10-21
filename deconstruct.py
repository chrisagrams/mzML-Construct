from tqdm import tqdm
from lxml import etree
from utils.mzml_utils import *
from utils.binary_utils import decode_binary
from utils.file_utils import export_to_parquet, export_to_binary


if __name__ == "__main__":
    source = 'input/test.mzML'
    output_xml = 'output/test.xml'
    output_parquet = 'output/test.parquet'

    tree = etree.parse(source)
    root = tree.getroot()
    nsmap = root.nsmap
    namespace = nsmap.get(None)

    records = []

    for spectrum in tqdm(root.findall(f'.//{{{namespace}}}spectrum'), desc='Processing mzML'):
        index = int(spectrum.get('index'))
        defaultArrayLength = int(spectrum.get('defaultArrayLength'))
        ms_level = get_ms_level(namespace, spectrum)
        retention_time = get_retention_time(namespace, spectrum)
        mz_array = None
        intensity_array = None

        for binaryDataArray in spectrum.findall(f'.//{{{namespace}}}binaryDataArray'):
            binaryDataArray.attrib['encodedLength'] = b'0'  # Set encoded length to 0
            binary_type = get_binary_type(namespace, binaryDataArray)
            source_format = get_source_format(namespace, binaryDataArray)
            source_compression = get_source_compression(namespace, binaryDataArray)
            binary = binaryDataArray.find(f'.//{{{namespace}}}binary')
            if binary is not None:
                arr = decode_binary(binary.text, source_format, source_compression)
                if binary_type == 'm/z array':
                    mz_array = arr
                elif binary_type == 'intensity array':
                    intensity_array = arr
                binary.text = ""  # Remove binary from XML

        if mz_array is not None and intensity_array is not None:
            for mz, intensity in zip(mz_array, intensity_array):
                records.append((index, retention_time, mz, intensity, ms_level))

    tree.write(output_xml)

    # export_to_parquet(records, output_parquet)

    export_to_binary(records, 'output/test.bin')
