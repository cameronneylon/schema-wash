# Copyright 2025 - Cameron Neylon
#
# Licensed under the MIT License (see LICENSE)

from typing import Union
from schemawash.utils import target_from_path

def filter_single_record(obj: dict, path: Union[list, str], value, desired_test_result)->bool:
    """Return True if element at path == value, unless desired result is False then False"""
    
    target, field = target_from_path(obj, path)
    result = target.get(field) == value
    return result == desired_test_result


def filter_record_element(obj: dict, path)->None:
    """Remove a  single element from a record at path if it exists"""

    target, field = target_from_path(obj, path)
    return target.pop(field, None)