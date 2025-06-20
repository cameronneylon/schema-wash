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
#
# Developer Notes
# 
# Cleaner functions for taking specified data elements and applying cleanup.
# Main cases are singletons to lists to create repeated records, conversion
# of mixed fields to strings and some more specialised functions for geo
# and other specific types
#
# Functions should all take JSON objects and a field modify the object in place
# as specified. Functions should test for type where appropriate and if that test
# fails return silently.

from typing import Union
from jsonlines.jsonlines import JSONScalar, JSONCollection, JSONValue

from schemawash.utils import target_from_path

def remove_nulls_from_list_field(obj: dict, path: Union[list, str]):
    target, field = target_from_path(obj, path)
    if target:
        value = target.get(field) or []
        target[field] = [x for x in value if x is not None]


def clean_empty_string_to_none(obj: dict, path: Union[list, str]):
    target, field = target_from_path(obj, path)
    if target:
        value = target.get(field)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            target[field] = None

        
# def clean_string_or_bool(value):
#     if value is None or (isinstance(value, str) and value.strip() == "") or isinstance(value, bool):
#         return None
#     return value


def format_geography_point(point):
    """Formats a geography point string if latitude and longitude are present."""
    lat, lng = point.get("pointLatitude"), point.get("pointLongitude")
    return f"POINT({lng} {lat})" if lat is not None and lng is not None else None



def normalize_to_string_or_none(obj:dict, path: Union[list, str])->None:
    """Normalizes a field to a string or returns None if the value is None or an empty string or a bool."""
    
    target, field = target_from_path(obj, path)
    if target:
        if isinstance(target, list):
            [normalize_to_string_or_none(instance, field) for instance in target]
            return
        value = target.get(field)
        if value is None or (isinstance(value, str) and not value.strip()) or (isinstance(value, bool)):
            target[field] = None
        else:
            target[field] = str(value)


def nested_array_to_dict(obj: dict, path: Union[list,str], keys: list):

    target, field = target_from_path(obj, path)
    old_array = target.get(field)
    new_array = []
    for item in old_array:
        assert isinstance(item, list)
        d = {k:v for k,v in zip(keys, item)}
        new_array.append(d)

    target[field] = new_array


def filter_non_empty_dicts(obj:dict, path:Union[list, str]):
    """Filters out empty dictionaries from a list of dicts."""

    target, field = target_from_path(obj, path)
    if target:
        arr = target.get(field) or []
        target[field] = [item for item in arr if not (isinstance(item, dict) and not item)]


def transform_geo_locations(obj:dict, path:Union[list, str]): 
    """Transforms and cleans geo-location data."""

    target, field = target_from_path(obj, path)
    if target:
        geoLocations = target.get(field)
    
        for loc in geoLocations:
            if "geoLocationPoint" in loc:
                loc["geoLocationPoint"] = format_geography_point(loc["geoLocationPoint"])

            # string and not float
            if "geoLocationBox" in loc:
                box = loc["geoLocationBox"]
                for key in ["northBoundLatitude", "southBoundLatitude", "eastBoundLongitude", "westBoundLongitude"]:
                    normalize_to_string_or_none(box, key)

            if "geoLocationPolygon" in loc:
                del loc["geoLocationPolygon"]

        filter_non_empty_dicts(target, field)


# def normalize_single_item_to_list(obj:dict, field: str):
#     """Normalizing of singleton str fields to lists with type checking.
    
#     """

#     item = obj.get(field, [])
#     if isinstance(item, str):
#         item = [item]

#     obj[field] = item


def normalize_related_item(obj: dict, path: Union[str,list]):
    """Normalizes items in related items array by converting to string or returning None."""
    
    target, field = target_from_path(obj, path)
    if target:
        related_items = target.get(field, [])
        for item in related_items:
            for key in ["firstPage", "lastPage", "publicationYear"]:
                value = item.get(key)
                if value is None or isinstance(value, list) or (isinstance(value, str) and value.strip() == ""):
                    item[key] = None
                else:
                    item[key] = str(value)

