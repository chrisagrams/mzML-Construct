from lxml import etree
from tqdm import tqdm
import pandas as pd
import numpy as np
from utils.mzml_utils import *
from utils.binary_utils import encode_binary, import_from_binary


def import_from_parquet(parquet_file):
    df = pd.read_parquet(parquet_file)

    consolidated_df = df.groupby(['spec_no', 'ret_time', 'ms_level']).agg({
        'mz': list,
        'int': list
    }).reset_index()

    return consolidated_df


if __name__ == "__main__":
    source = 'output/test.xml'
    source_binary = 'output/test.parquet'
    output_file = 'output/test.mzML'

    tree = etree.parse(source)
    root = tree.getroot()
    nsmap = root.nsmap
    namespace = nsmap.get(None)

    # df = import_from_parquet(source)
    df = import_from_binary('output/test.bin')

    for spectrum in tqdm(root.findall(f'.//{{{namespace}}}spectrum'), desc='Constructing mzML'):
        index = int(spectrum.get('index'))
        spectrum.set('defaultArrayLength', str(len(df['mz'][index])))

        for binaryDataArray in spectrum.findall(f'.//{{{namespace}}}binaryDataArray'):
            binary_type = get_binary_type(namespace, binaryDataArray)
            source_format = get_source_format(namespace, binaryDataArray)
            source_compression = get_source_compression(namespace, binaryDataArray)
            binary = binaryDataArray.find(f'.//{{{namespace}}}binary')
            if binary is not None:
                if binary_type == 'm/z array':
                    binary.text = encode_binary(np.array(df['mz'][index]), source_format, source_compression)
                    binaryDataArray.attrib['encodedLength'] = str(len(binary.text))
                elif binary_type == 'intensity array':
                    binary.text = encode_binary(np.array(df['int'][index]), source_format, source_compression)
                    binaryDataArray.attrib['encodedLength'] = str(len(binary.text))

    tree.write(output_file, pretty_print=True, xml_declaration=True, encoding='utf-8')
