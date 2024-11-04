# mzML Construct and Deconstruct

A Python program designed to split mzML files into XML and binary formats and reconstruct them back into mzML. It is particularly useful for testing and validating binary transformations of mzML files.

## Features
- **Deconstruct**: Convert mzML files into XML and binary, Parquet, or NPY formats.
- **Reconstruct**: Rebuild mzML files from XML and binary/Parquet/NPY files.

## Installation
Make sure you have Python installed and the necessary dependencies:
```sh
pip install -r requirements.txt
```
Alternatively, build and use the Docker container as described below.


## Usage
Deconstruct mzML to XML and binary/Parquet/npy formats:
``` sh
# Usage: deconstruct.py [-h] [-x OUTPUT_XML] -f {parquet,bin,npy} input output_dir
# Parquet format:
python deconstruct.py input.mzML output/ -f parquet
# Binary format:
python deconstruct.py input.mzML output/ -f bin
# NPY format:
python deconstruct.py input.mzML output/ -f npy

```

Reconstruct mzML from XML and binary/Parquet:
```sh
#usage: construct.py [-h] xml_file data_file output_file
# Using binary file:
python construct.py input.xml input.bin output.mzML
# Using Parquet file:
python construct.py input.xml input.parquet output.mzML
# Using NPY file:
python construct.py input.xml input.npy output.mzML
```

### Command-Line Options
- `-h` : Show help message.
- `-x OUTPUT_XML` : Specify output XML file path.
- `-f {parquet,bin,npy}`: Specify the export binary format.

### Docker Usage
You can also run the deconstruction and construction processes using Docker:
1. Build the Docker image:
``` sh
docker build -t mzml-construct .
```
2. Deconstruct an mzML file:
```sh
docker run --rm -v $(pwd):/data mzml-construct python deconstruct.py /data/input.mzML /data/output/ -f bin
```
3. Reconstruct an mzML file:
```sh
docker run --rm -v $(pwd):/data mzml-construct python construct.py /data/input.xml /data/input.bin /data/output.mzML
```
- You can replace `input.bin` with `input.parquet` or `input.npy` if needed.

> **Note:** Ensure your files are located in the current working directory, as the Docker setup mounts them into the container at /data. Adjust paths if your files are located elsewhere.

## License
This project is licensed under the MIT License.