# AWS S3 Controller

A Python module for efficient management and interaction with AWS S3 buckets. This module provides a comprehensive set of tools for file operations, data handling, and bucket management.

## Features

- **File Scanning**: Search files in S3 buckets and local directories using regex patterns
- **File Transfer**: Upload, download, and relocate files between S3 buckets and local directories
- **Data Processing**: Read CSV and Excel files directly from S3 into pandas DataFrames
- **Bucket Management**: Create and manage S3 bucket structure
- **Special Operations**: Handle specific use cases like timeseries data processing

## Installation

```bash
pip install -r requirements.txt
```

## Module Structure

The module is organized into several specialized components:

- `s3_scanner.py`: File search functionality in S3 buckets and local directories
- `s3_transfer.py`: File transfer operations between S3 and local storage
- `s3_dataframe_reader.py`: Functions for reading files into pandas DataFrames
- `s3_structure.py`: S3 bucket structure management
- `s3_special_operations.py`: Special purpose functions for specific operations

## Usage Examples

### Scanning Files

```python
from aws_s3_controller import scan_files_in_bucket_by_regex

# Find all CSV files in a bucket
files = scan_files_in_bucket_by_regex(
    bucket="my-bucket",
    bucket_prefix="data",
    regex=r".*\.csv$",
    option="key"
)
```

### Transferring Files

```python
from aws_s3_controller import download_files_from_s3, upload_files_to_s3

# Download files matching a pattern
download_files_from_s3(
    bucket="my-bucket",
    regex=r".*\.csv$",
    file_folder_local="./downloads",
    bucket_prefix="data"
)

# Upload files to S3
upload_files_to_s3(
    file_folder_local="./uploads",
    regex=r".*\.xlsx$",
    bucket="my-bucket",
    bucket_prefix="excel-files"
)
```

### Reading Data

```python
from aws_s3_controller import open_df_in_bucket, open_excel_in_bucket

# Read CSV file
df = open_df_in_bucket(
    bucket="my-bucket",
    bucket_prefix="data",
    file_name="example.csv"
)

# Read Excel file
df = open_excel_in_bucket(
    bucket="my-bucket",
    bucket_prefix="excel",
    file_name="example.xlsx"
)
```

## Dependencies

- boto3
- pandas
- python-dotenv
- xlrd (for Excel file support)
- shining_pebbles

## Configuration

1. Create a `.env` file in your project root
2. Add your AWS credentials:
```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=your_region
```

## Documentation

Detailed documentation is available in the `doc` directory:
- `design.md`: Project design documentation
- `context.md`: Project context and progress
- `commands-cascade.md`: Command history and functionality

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes with descriptive commit messages
4. Push to your branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
