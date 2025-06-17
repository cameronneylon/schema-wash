# Copyright 2025 - Cameron Neylon
#
# Licensed under the MIT License (see LICENSE)

from typing import Union
from jsonlines.jsonlines import JSONScalar, JSONCollection, JSONValue
from schemawash.utils import target_from_path

def filter_single_record(obj: dict, path: Union[list, str], value: Union[JSONScalar, list], desired_test_result: bool=True)->bool:
    """Return True if element at path == value, unless desired result is False then False"""
    
    target, field = target_from_path(obj, path)
    result = target.get(field)

    if isinstance(value, list):
        test = True in [result == test for test in value]
    else:
        test = result == value
    return test == desired_test_result


def filter_record_element(obj: dict, path)->None:
    """Remove and return a single element from a record at path if it exists"""

    target, field = target_from_path(obj, path)
    return target.pop(field, None)