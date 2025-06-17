# Copyright 2025 - Cameron Neylon
#
# Licensed under the MIT License (see LICENSE)
# 
# This library is substantially derived from 
# https://github.com/The-Academic-Observatory/academic-observatory-workflows
# and specifically the Datacite workflow which is
#
# Copyright 2020-2025 Curtin University
# Copyright 2024-2025 UC Curation Center (California Digital Library)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License._workers

import argparse
import os

from schemawash.lib import check_directory, check_yaml_file, generate_schema_for_dataset

if __name__ == "__main__":
    """Command line tool to transform DataCite and generate a first pass at a schema"""

    parser = argparse.ArgumentParser(description="Process OpenAlex entities.")

    # Required arguments
    parser.add_argument("input_folder", type=check_directory, help="The input folder path")
    parser.add_argument("output_folder", type=check_directory, help="The output folder path")
    parser.add_argument("config_path", type=check_yaml_file, help="Path to YAML config file for data source")
    
    # Optional arguments with default value from os.cpu_count()
    parser.add_argument(
        "--max_workers",
        type=int,
        default=os.cpu_count(),
        help="The maximum number of workers (default: number of CPUs)",
    )
    parser.add_argument(
        "--schema_keep_nulls",
        type=bool,
        default=True,
        help="Bigquery Schema Generator option to keep elements that are never populated (default: True)"
    )
    parser.add_argument(
        '--file_suffix',
        type=str,
        default='jsonl.gz',
        help='File suffix for the target files for processing (default: ".jsonl.gz")'
    )

    # Parse the arguments
    args = parser.parse_args()

    generate_schema_for_dataset(
        args.input_folder, 
        args.output_folder, 
        args.config_path, 
        args.max_workers,
        args.schema_keep_nulls, 
        args.file_suffix)