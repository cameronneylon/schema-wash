# Copyright 2025 - Cameron Neylon
#
# Licensed under the MIT License (see LICENSE)

from typing import Union
from utils import target_from_path

def filter_single_record(obj: dict, path: Union[list, str], value)->bool:
    """Return True if element at path == value"""
    
    target, field = target_from_path(obj, path)
    return target.get(field) == value


def filter_record_element(obj: dict, path)->None:
    """Remove a  single element from a record at path if it exists"""

    target, field = target_from_path(obj, path)
    return target.pop(field, None)