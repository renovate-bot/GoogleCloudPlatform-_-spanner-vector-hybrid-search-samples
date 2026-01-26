# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utility functions for generating range-based split points."""
import re
import uuid as uuid_module
from typing import Optional

from models import (
    SupportedRangeType,
    RangeValidationResult,
    EntityKeySchema,
)


# UUID regex pattern for canonical format (8-4-4-4-12 with lowercase hex)
UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE
)


def is_valid_uuid(value: str) -> bool:
    """Check if a value is a valid canonical UUID format.

    Args:
        value: String to validate

    Returns:
        True if the value is a valid UUID in canonical format (36 chars with dashes)
    """
    if len(value) != 36:
        return False
    return bool(UUID_PATTERN.match(value))


def uuid_to_int(uuid_str: str) -> int:
    """Convert a UUID string to its 128-bit integer representation.

    Args:
        uuid_str: UUID string in canonical format

    Returns:
        128-bit integer representation of the UUID

    Raises:
        ValueError: If the UUID string is invalid
    """
    return uuid_module.UUID(uuid_str).int


def int_to_uuid(value: int) -> str:
    """Convert a 128-bit integer to a canonical UUID string.

    Args:
        value: 128-bit integer

    Returns:
        UUID string in lowercase canonical format
    """
    return str(uuid_module.UUID(int=value))


def generate_int64_range_splits(
    start: int,
    end: int,
    num_splits: int,
    include_boundaries: bool = True
) -> tuple[list[str], list[str]]:
    """Generate evenly distributed INT64 split values.

    Args:
        start: Start value of the range (inclusive if include_boundaries)
        end: End value of the range (inclusive if include_boundaries)
        num_splits: Number of split points to generate
        include_boundaries: Whether to include start and end values

    Returns:
        Tuple of (generated_values, warnings)

    Raises:
        ValueError: If start >= end or num_splits < 2
    """
    warnings: list[str] = []

    if start >= end:
        raise ValueError("Start value must be less than end value")
    if num_splits < 2:
        raise ValueError("Number of splits must be at least 2")

    values: list[str] = []

    if include_boundaries:
        # Calculate step size for evenly distributed points
        # With include_boundaries, we want num_splits points including start and end
        step = (end - start) / (num_splits - 1)

        for i in range(num_splits):
            value = start + int(step * i)
            # Ensure we hit exact boundaries
            if i == 0:
                value = start
            elif i == num_splits - 1:
                value = end
            values.append(str(value))

        # Check for rounding issues
        actual_end = int(start + step * (num_splits - 1))
        if actual_end != end:
            warnings.append("End boundary adjusted due to integer division rounding")
    else:
        # Exclude boundaries - generate points between start and end
        step = (end - start) / (num_splits + 1)

        for i in range(1, num_splits + 1):
            value = start + int(step * i)
            values.append(str(value))

    return values, warnings


def generate_uuid_range_splits(
    start_uuid: str,
    end_uuid: str,
    num_splits: int,
    include_boundaries: bool = True
) -> tuple[list[str], list[str]]:
    """Generate evenly distributed UUID split values.

    Converts UUIDs to 128-bit integers, calculates evenly spaced points,
    and converts back to canonical UUID format.

    Args:
        start_uuid: Start UUID string (inclusive if include_boundaries)
        end_uuid: End UUID string (inclusive if include_boundaries)
        num_splits: Number of split points to generate
        include_boundaries: Whether to include start and end values

    Returns:
        Tuple of (generated_values, warnings)

    Raises:
        ValueError: If UUIDs are invalid, start >= end, or num_splits < 2
    """
    warnings: list[str] = []

    if not is_valid_uuid(start_uuid):
        raise ValueError(f"Value '{start_uuid}' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
    if not is_valid_uuid(end_uuid):
        raise ValueError(f"Value '{end_uuid}' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")

    start_int = uuid_to_int(start_uuid)
    end_int = uuid_to_int(end_uuid)

    if start_int >= end_int:
        raise ValueError("Start value must be less than end value")
    if num_splits < 2:
        raise ValueError("Number of splits must be at least 2")

    values: list[str] = []

    if include_boundaries:
        # Calculate step size for evenly distributed points
        step = (end_int - start_int) / (num_splits - 1)

        for i in range(num_splits):
            if i == 0:
                value_int = start_int
            elif i == num_splits - 1:
                value_int = end_int
            else:
                value_int = start_int + int(step * i)
            values.append(int_to_uuid(value_int))
    else:
        # Exclude boundaries - generate points between start and end
        step = (end_int - start_int) / (num_splits + 1)

        for i in range(1, num_splits + 1):
            value_int = start_int + int(step * i)
            values.append(int_to_uuid(value_int))

    return values, warnings


def detect_range_type(
    spanner_type: str,
    sample_value: Optional[str] = None
) -> tuple[Optional[SupportedRangeType], Optional[str]]:
    """Determine if a column type supports range splits and which type.

    Args:
        spanner_type: The Spanner column type (e.g., "INT64", "STRING(36)", "BYTES(16)")
        sample_value: Optional sample value to validate UUID format

    Returns:
        Tuple of (SupportedRangeType or None, error_message or None)
    """
    # Check for INT64
    if spanner_type.upper() == "INT64":
        return SupportedRangeType.INT64, None

    # Check for STRING type (UUID requires 36 chars: 8-4-4-4-12 with dashes)
    if spanner_type.upper().startswith("STRING"):
        # Extract length from STRING(n) format
        match = re.match(r'STRING\((\d+|MAX)\)', spanner_type, re.IGNORECASE)
        if match:
            length_str = match.group(1)
            if length_str.upper() == "MAX":
                length = 36  # MAX is sufficient for UUIDs
            else:
                length = int(length_str)

            if length <= 35:
                return None, f"Column length ({length}) too short for UUIDs (need greater than 35)"

            # If a sample value is provided, validate it's a UUID
            if sample_value is not None:
                if not is_valid_uuid(sample_value):
                    return None, f"Value '{sample_value}' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"

            return SupportedRangeType.STRING_UUID, None
        else:
            return None, f"Could not parse STRING type: {spanner_type}"

    # Check for BYTES type (UUID requires 16 bytes)
    if spanner_type.upper().startswith("BYTES"):
        # Extract length from BYTES(n) format
        match = re.match(r'BYTES\((\d+|MAX)\)', spanner_type, re.IGNORECASE)
        if match:
            length_str = match.group(1)
            if length_str.upper() == "MAX":
                length = 16  # MAX is sufficient for UUIDs
            else:
                length = int(length_str)

            if length <= 15:
                return None, f"Column length ({length}) too short for UUIDs (need greater than 15)"

            # If a sample value is provided, validate it's a UUID
            if sample_value is not None:
                if not is_valid_uuid(sample_value):
                    return None, f"Value '{sample_value}' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"

            return SupportedRangeType.BYTES_UUID, None
        else:
            return None, f"Could not parse BYTES type: {spanner_type}"

    # Other types are not supported
    return None, f"Column type '{spanner_type}' not supported. Supported: INT64, STRING(>35) with UUIDs, BYTES(>15) with UUIDs."


def validate_range_request(
    schema: EntityKeySchema,
    start_value: str,
    end_value: str
) -> RangeValidationResult:
    """Validate a range split request against an entity schema.

    Args:
        schema: The entity's key schema
        start_value: Start value for the range
        end_value: End value for the range

    Returns:
        RangeValidationResult with validation status and detected type
    """
    # Check for composite keys
    if schema.is_composite:
        return RangeValidationResult(
            is_valid=False,
            range_type=None,
            error_message="Range splits are not supported for composite keys. Please add splits individually."
        )

    # Get the single key column
    if not schema.key_columns:
        return RangeValidationResult(
            is_valid=False,
            range_type=None,
            error_message="No key columns found in schema"
        )

    key_column = schema.key_columns[0]

    # Detect range type
    range_type, error = detect_range_type(key_column.spanner_type, start_value)

    if error:
        return RangeValidationResult(
            is_valid=False,
            range_type=None,
            error_message=error
        )

    # Validate the values based on detected type
    if range_type == SupportedRangeType.INT64:
        try:
            start_int = int(start_value)
            end_int = int(end_value)
            if start_int >= end_int:
                return RangeValidationResult(
                    is_valid=False,
                    range_type=range_type,
                    error_message="Start value must be less than end value"
                )
        except ValueError:
            return RangeValidationResult(
                is_valid=False,
                range_type=range_type,
                error_message=f"Invalid integer value(s): start='{start_value}', end='{end_value}'"
            )

    elif range_type in (SupportedRangeType.STRING_UUID, SupportedRangeType.BYTES_UUID):
        if not is_valid_uuid(start_value):
            return RangeValidationResult(
                is_valid=False,
                range_type=range_type,
                error_message=f"Value '{start_value}' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"
            )
        if not is_valid_uuid(end_value):
            return RangeValidationResult(
                is_valid=False,
                range_type=range_type,
                error_message=f"Value '{end_value}' is not a valid UUID format (expected: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)"
            )

        # Compare UUID values lexicographically (which matches their integer representation for canonical UUIDs)
        start_int = uuid_to_int(start_value)
        end_int = uuid_to_int(end_value)
        if start_int >= end_int:
            return RangeValidationResult(
                is_valid=False,
                range_type=range_type,
                error_message="Start value must be less than end value"
            )

    return RangeValidationResult(
        is_valid=True,
        range_type=range_type,
        error_message=None
    )


def generate_range_splits(
    range_type: SupportedRangeType,
    start_value: str,
    end_value: str,
    num_splits: int,
    include_boundaries: bool = True
) -> tuple[list[str], list[str]]:
    """Generate range split values based on the detected type.

    Args:
        range_type: The type of range (INT64, STRING_UUID, or BYTES_UUID)
        start_value: Start value for the range
        end_value: End value for the range
        num_splits: Number of split points to generate
        include_boundaries: Whether to include start and end values

    Returns:
        Tuple of (generated_values, warnings)

    Raises:
        ValueError: If validation fails
    """
    if range_type == SupportedRangeType.INT64:
        return generate_int64_range_splits(
            start=int(start_value),
            end=int(end_value),
            num_splits=num_splits,
            include_boundaries=include_boundaries
        )
    elif range_type in (SupportedRangeType.STRING_UUID, SupportedRangeType.BYTES_UUID):
        return generate_uuid_range_splits(
            start_uuid=start_value,
            end_uuid=end_value,
            num_splits=num_splits,
            include_boundaries=include_boundaries
        )
    else:
        raise ValueError(f"Unsupported range type: {range_type}")
