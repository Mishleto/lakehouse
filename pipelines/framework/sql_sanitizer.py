"""
sql_sanitizer.py
-------------------
Stateless utility for validating object names and avoiding SQL injection:

Public API
----------
validate_object_identifier(db_object_name)

"""

import re

def validate_object_identifier(db_object_name: str) -> None:
    """
    Validates that a string is a safe SQL object (table, view, function, schema...) identifier.
    Allows dot-separated parts (e.g. catalog.schema.table),
    each part being alphanumeric/underscores only.
    Does not check if the object really exists in a given catalog!
    Only if the name is valid and could be used safely!
    Raises ValueError if invalid.
    """
    
    parts = db_object_name.split(".")
    valid_part = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
    if not parts or not all(valid_part.match(p) for p in parts):
        raise ValueError(f"Invalid DB object identifier: {db_object_name!r}")
    
