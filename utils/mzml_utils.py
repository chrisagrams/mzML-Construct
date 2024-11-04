from lxml import etree


def get_binary_type(namespace: str, binaryDataArray: etree._Element) -> str:
    cvParams = binaryDataArray.findall(f".//{{{namespace}}}cvParam")
    for cv_param in cvParams:
        if cv_param.attrib["accession"] == "MS:1000514":
            return "m/z array"
        elif cv_param.attrib["accession"] == "MS:1000515":
            return "intensity array"
    raise ValueError(f"Could not find binary type.")


def get_source_format(namespace: str, binaryDataArray: etree._Element) -> str:
    cvParams = binaryDataArray.findall(f".//{{{namespace}}}cvParam")
    for cv_param in cvParams:
        if cv_param.attrib["accession"] == "MS:1000521":  # 32f
            return "f"
        elif cv_param.attrib["accession"] == "MS:1000523":  # 64d
            return "d"
    raise NotImplementedError("Source format not implemented or found.")


def get_source_compression(namespace: str, binaryDataArray: etree._Element) -> bool:
    cvParams = binaryDataArray.findall(f".//{{{namespace}}}cvParam")
    for cv_param in cvParams:
        if cv_param.attrib["accession"] == "MS:1000574":  # zlib
            return True
        elif cv_param.attrib["accession"] == "MS:1000576":  # no compression
            return False
    raise ValueError("No source compression found.")


def get_ms_level(namespace: str, spectrum: etree._Element) -> int:
    if spectrum is not None:
        cv_params = spectrum.findall(f".//{{{namespace}}}cvParam")
        for cv_param in cv_params:
            if cv_param.attrib["accession"] == "MS:1000511":
                return int(cv_param.attrib["value"])
    raise ValueError


def get_retention_time(namespace: str, spectrum: etree._Element) -> float:
    cv_params = spectrum.findall(f".//{{{namespace}}}cvParam")
    for cv_param in cv_params:
        if cv_param.attrib["accession"] == "MS:1000016":  # Retention time
            return float(cv_param.attrib["value"])
    raise ValueError("Retention time not found.")
