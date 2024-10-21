# mzML Construct and Deconstruct

A Python program designed to split mzML files into XML and binary formats and reconstruct them back into mzML. It is particularly useful for testing and validating binary transformations of mzML files.

## Features
- **Deconstruct**: Convert mzML files into XML and binary (or Parquet) formats.
- **Reconstruct**: Rebuild mzML files from XML and binary/Parquet files.

## Installation
Make sure you have Python installed and the necessary dependencies:
```sh
pip install -r requirements.txt
```
Alternatively, build and use the Docker container as described below.


## Usage
Deconstruct mzML to XML and binary/Parquet:
``` sh
# Usage: deconstruct.py [-h] [-x OUTPUT_XML] [-p OUTPUT_PARQUET] [-b OUTPUT_BINARY] input output_dir
python deconstruct.py input.mzml output/
```

Reconstruct mzML from XML and binary/Parquet:
```sh
#usage: construct.py [-h] xml_file data_file output_file
python construct.py input.xml input.bin output.mzML
# Alternatively:
python construct.py input.xml input.parquet output.mzML
```

### Command-Line Options
- `-h` : Show help message.
- `-x OUTPUT_XML` : Specify output XML file path.
- `-p OUTPUT_PARQUET` : Specify output Parquet file path.
- `-b OUTPUT_BINARY` : Specify output binary file path.

### Docker Usage
You can also run the deconstruction and construction processes using Docker:
1. Build the Docker image:
``` sh
docker build -t mzml-construct .
```
2. Deconstruct an mzML file:
```sh
docker run --rm -v $(pwd):/data mzml-construct python deconstruct.py /data/input.mzML /data/output/
```
3. Reconstruct an mzML file:
```sh
docker run --rm -v $(pwd):/data mzml-construct python construct.py /data/input.xml /data/input.bin /data/output.mzML
```
- You can replace input.bin with `input.parquet` if needed.

> **Note:** Ensure your files are located in the current working directory, as the Docker setup mounts them into the container at /data. Adjust paths if your files are located elsewhere.

## License
This project is licensed under the MIT License.