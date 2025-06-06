# Copyright 2025 - Cameron Neylon
#
# Licensed under the MIT License (see LICENSE)

from typing import Union, Any
from jsonlines.jsonlines import JSONScalar, JSONCollection, JSONValue


def target_from_path(obj:dict, path: Union[list,str])->(tuple[dict, Any] | tuple[None, None]):
    """Given an object dict and a path or field, return] the target object and field"""
    if isinstance(path, str):
        path = [path]

    if len(path) == 1:
        if path[0] in obj:
            return obj, path[0]
        else:
            return None, None

    else:
        target = obj
        for element in path[0:-1]:
            target = target.get(element)
            if not target:
                return None, None

        return target, path[-1]
