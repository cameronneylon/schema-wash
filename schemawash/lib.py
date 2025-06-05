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
# limitations under the License.


from __future__ import annotations

import argparse
import glob
import json
import yaml
import logging
import os
import datetime
from collections import OrderedDict
from concurrent.futures import as_completed, ProcessPoolExecutor
from json import JSONEncoder
from pathlib import Path
from typing import Any, List, Tuple, Union

import json_lines
import jsonlines
from bigquery_schema_generator.generate_schema import flatten_schema_map, SchemaGenerator

import cleaner_functions
import filter_records

def yield_jsonl(file_path: str):
    """Return or yield row of a JSON lines file as a dictionary. If the file
    is gz compressed then it will be extracted.

    :param file_path: the path to the JSON lines file.
    :return: generator.
    """

    with json_lines.open(file_path) as file:
        for row in file:
            yield row


def merge_schema_maps(to_add: OrderedDict, old: OrderedDict) -> OrderedDict:
    """Using the SchemaGenerator from the bigquery_schema_generator library, merge the schemas found
    when from scanning through files into one large nested OrderedDict.

    :param to_add: The incoming schema to add to the existing "old" schema.
    :param old: The existing old schema with previously populated values.
    :return: The old schema with newly added fields.
    """

    schema_generator = SchemaGenerator()

    if old:
        # Loop through the fields to add to the schema
        for key, value in to_add.items():
            if key in old:
                # Merge existing fields together.
                old[key] = schema_generator.merge_schema_entry(old_schema_entry=old[key], new_schema_entry=value)
            else:
                # New top level field is added.
                old[key] = value
    else:
        # Initialise it with first result if it is empty
        old = to_add.copy()

    return old


def flatten_schema(schema_map: OrderedDict) -> dict:
    """A quick trick using the JSON encoder and load string function to convert from a nested
    OrderedDict object to a regular dictionary.

    :param schema_map: The generated schema from SchemaGenerator.
    :return schema: A Bigquery style schema."""

    encoded_schema = JSONEncoder().encode(
        flatten_schema_map(
            schema_map,
            keep_nulls=False,
            sorted_schema=True,
            infer_mode=True,
            input_format="json",
        )
    )

    return json.loads(encoded_schema)


def sort_schema(input_file: Path):
    def sort_schema_func(schema):
        # Sort schema entries by name and sort the fields of each entry by key_order
        key_order = ["name", "type", "mode", "description", "fields"]
        sorted_schema = [
            {k: field[k] for k in key_order if k in field} for field in sorted(schema, key=lambda x: x["name"])
        ]

        # Sort the fields recursively
        for field in sorted_schema:
            if field.get("type") == "RECORD" and "fields" in field:
                field["fields"] = sort_schema_func(field["fields"])

        return sorted_schema

    # Load the JSON schema from a string
    with open(input_file, mode="r", encoding='utf-8') as f:
        data = json.load(f)

    # Sort the schema
    sorted_json_schema = sort_schema_func(data)

    # Save the schema
    with open(input_file, mode="w", encoding='utf-8') as f:
        json.dump(sorted_json_schema, f, indent=2)


def list_import_files(folder_path, file_suffix):
    """Recursively identify all files within a folder that match the specified suffix"""

    filepaths = os.path.join(folder_path, '**', f'*{file_suffix}')
    filelist = glob.glob(filepaths, recursive=True)
    return filelist


def clean_object(obj, cleaners=[]):
    """Run the specified list of cleaner functions from config over an individual object"""
    
    for cleaner in cleaners:
        cleaner_function = getattr(cleaner_functions, cleaner.get('function'))
        params = cleaner.get('params')
        cleaner_function(obj,**params)


def transform(input_path: str, 
              output_path: str, 
              config, 
              schema_keep_nulls: bool=True) -> Tuple[str, bool, OrderedDict, list]:
    """Process a chunk of files as provided by the ProcessPoolExecutor"""
    
    # Parse the config elements
    filters = config.get('filter_records')
    cleaners = config.get('cleaners')
    
    # Initialise the schema generator.
    schema_map = OrderedDict()
    schema_generator = SchemaGenerator(input_format="dict",
                                       keep_nulls=schema_keep_nulls)

    # Make base folder
    base_folder = os.path.dirname(output_path)
    os.makedirs(base_folder, exist_ok=True)

    # print(f"generate_schema {input_path}")
    empty_file = True
    lines = 0
    with open(output_path, mode='w', encoding='utf-8') as f:
        with jsonlines.Writer(f) as writer:
            for obj in yield_jsonl(input_path):
                if filters:
                    # Test each filter placing bool result in an array
                    if False in [
                        filter_records.filter_single_record(
                            obj=obj,
                            path=filter.get('path'),
                            value=filter.get('value'),
                            desired_test_result=filter.get('desired_test_result', True)
                            ) for filter in filters
                            ]:
                        continue
                
                empty_file = False
                if cleaners:
                    clean_object(obj, cleaners)
                writer.write(obj)
                lines += 1

                # Wrap this in a try and pass so that it doesn't
                # cause the transform step to fail unexpectedly.
                try:
                    schema_generator.deduce_schema_for_record(obj, schema_map)
                except Exception:
                    pass

    if empty_file:
        logging.info(f"Deleting file as it is empty: {output_path}")
        os.remove(output_path)

    return input_path, empty_file, schema_map, schema_generator.error_logs, lines


def get_chunks(*, input_list: List[Any], chunk_size: int = 8) -> List[Any]:
    """Generator that splits a list into chunks of a fixed size.

    :param input_list: Input list.
    :param chunk_size: Size of chunks.
    :return: The next chunk from the input list.
    """

    n = len(input_list)
    for i in range(0, n, chunk_size):
        yield input_list[i : i + chunk_size]


def generate_schema_for_dataset(input_folder: Path, 
                                output_folder: Path, 
                                config_path: Union[Path, str],
                                max_workers: int=os.cpu_count(),
                                schema_keep_nulls: bool=True,
                                file_suffix: str = '.jsonl.gz'
                                ):
    merged_schema_map = OrderedDict()
    i = 1
    lines = 0
    files = list_import_files(input_folder, file_suffix=file_suffix)
    total_files = len(files)
    config_path = Path(config_path)
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    print(datetime.datetime.now())
    print(f'Starting run of {input_folder}')
    print(f'Config path is {config_path}')

    for c, chunk in enumerate(get_chunks(input_list=files, chunk_size=500)):
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            for input_path in chunk:
                output_path = str(output_folder / Path(input_path).relative_to(input_folder)).replace(
                    file_suffix, ".jsonl"
                )
                futures.append(executor.submit(transform, input_path, output_path, config, schema_keep_nulls))

            
            for future in as_completed(futures):
                input_path, empty_file, schema_map, schema_error, written_lines = future.result()
                if not empty_file:
                    lines += written_lines
                    if schema_error:
                        msg = f"File {input_path}: {schema_error}"
                        with open(input_folder / "errors.txt", mode="a", encoding='utf-8') as f:
                            f.write(f"{msg}\n")
                        print(msg)

                    # Merge the schemas from each process. Each data file could have more fields than others.
                    try:
                        merged_schema_map = merge_schema_maps(to_add=schema_map, old=merged_schema_map)
                    except Exception as e:
                        print(f"merge_schema_maps error: {e}")

                percent = i / total_files * 100
                print(f"Progress: {i} / {total_files}, {percent:.2f}%. {lines} lines written.")
                i += 1

    # Flatten schema from nested OrderedDicts to a regular Bigquery schema.
    merged_schema = flatten_schema(schema_map=merged_schema_map)

    # Save schema to file
    generated_schema_path = os.path.join(input_folder, "schema.json")
    with open(generated_schema_path, mode="w", encoding='utf-8') as f_out:
        json.dump(merged_schema, f_out, indent=2)

    sort_schema(Path(generated_schema_path))


def check_directory(path):
    """Check if the provided path is a valid directory."""
    if not Path(path).is_dir():
        raise argparse.ArgumentTypeError(f"The directory {path} does not exist.")
    return Path(path)


def check_yaml_file(path):
    """
    Check if the provided path lands at a file with yaml extension
    
    TODO: Check if possible to validate file schema
    """

    p = Path(path)
    if not (p.is_file() and p.suffix == '.yaml'):
        raise argparse.ArgumentTypeError(f"The config path {path} does not exist or is not a .yaml file")
    return p

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