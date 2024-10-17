import struct
import base64
import zlib
from lxml import etree
import numpy as np


def get_source_format(namespace: str, binaryDataArray: etree._Element) -> str:
    cvParams = binaryDataArray.findall(f'.//{{{namespace}}}cvParam')
    for cv_param in cvParams:
        if cv_param.attrib['accession'] == 'MS:1000521':  # 32f
            return 'f'
        elif cv_param.attrib['accession'] == 'MS:1000523':  # 64d
            return 'd'
    raise NotImplementedError("Source format not implemented or found.")


def get_source_compression(namespace: str, binaryDataArray: etree._Element) -> bool:
    cvParams = binaryDataArray.findall(f'.//{{{namespace}}}cvParam')
    for cv_param in cvParams:
        if cv_param.attrib['accession'] == 'MS:1000574':  # zlib
            return True
        elif cv_param.attrib['accession'] == 'MS:1000576':  # no compression
            return False
    raise ValueError("No source compression found.")


def get_ms_level(namespace: str, spectrum: etree._Element) -> int:
    if spectrum is not None:
        cv_params = spectrum.findall(f'.//{{{namespace}}}cvParam')
        for cv_param in cv_params:
            if cv_param.attrib["accession"] == 'MS:1000511':
                return int(cv_param.attrib['value'])
    raise ValueError


def decode_binary(text: str, source_format: str, source_compression: bool) -> np.ndarray:
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


if __name__ == "__main__":
    source = 'input/test.mzML'
    output_xml = 'output/test.xml'
    output_binary = 'output/test.bin'

    tree = etree.parse(source)
    root = tree.getroot()
    nsmap = root.nsmap
    namespace = nsmap.get(None)

    for spectrum in root.findall(f'.//{{{namespace}}}spectrum'):
        index = int(spectrum.get('index'))
        defaultArrayLength = int(spectrum.get('defaultArrayLength'))
        ms_level = get_ms_level(namespace, spectrum)
        for binaryDataArray in spectrum.findall(f'.//{{{namespace}}}binaryDataArray'):
            binaryDataArray.attrib['encodedLength'] = b'0'  # Set encoded length to 0
            source_format = get_source_format(namespace, binaryDataArray)
            source_compression = get_source_compression(namespace, binaryDataArray)
            binary = binaryDataArray.find(f'.//{{{namespace}}}binary')
            if binary is not None:
                arr = decode_binary(binary.text, source_format, source_compression)
                binary.text = None  # Remove binary from XML

    tree.write(output_xml)

