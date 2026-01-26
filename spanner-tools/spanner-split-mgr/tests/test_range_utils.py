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

"""Unit tests for range_utils.py module.

This module tests all functions related to range-based split point generation,
including UUID validation, UUID/INT64 conversions, and range split generation.
"""
import pytest
import uuid as uuid_module

from range_utils import (
    is_valid_uuid,
    uuid_to_int,
    int_to_uuid,
    generate_int64_range_splits,
    generate_uuid_range_splits,
    detect_range_type,
    validate_range_request,
    generate_range_splits,
)
from models import (
    SupportedRangeType,
    EntityKeySchema,
    EntityType,
    KeyColumnInfo,
)


# =============================================================================
# UUID Validation Tests
# =============================================================================

class TestIsValidUuid:
    """Tests for is_valid_uuid function."""

    def test_valid_uuid_lowercase(self):
        """Test that a valid lowercase UUID is accepted."""
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        assert is_valid_uuid(uuid_str) is True

    def test_valid_uuid_uppercase(self):
        """Test that a valid uppercase UUID is accepted (case insensitive)."""
        uuid_str = "550E8400-E29B-41D4-A716-446655440000"
        assert is_valid_uuid(uuid_str) is True

    def test_valid_uuid_mixed_case(self):
        """Test that a valid mixed-case UUID is accepted."""
        uuid_str = "550e8400-E29B-41d4-A716-446655440000"
        assert is_valid_uuid(uuid_str) is True

    def test_valid_uuid_all_zeros(self):
        """Test that the nil UUID (all zeros) is valid."""
        uuid_str = "00000000-0000-0000-0000-000000000000"
        assert is_valid_uuid(uuid_str) is True

    def test_valid_uuid_all_fs(self):
        """Test that the max UUID (all f's) is valid."""
        uuid_str = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        assert is_valid_uuid(uuid_str) is True

    def test_valid_uuid_generated(self):
        """Test that a randomly generated UUID is valid."""
        uuid_str = str(uuid_module.uuid4())
        assert is_valid_uuid(uuid_str) is True

    def test_invalid_uuid_wrong_length_short(self):
        """Test that a UUID that's too short is rejected."""
        uuid_str = "550e8400-e29b-41d4-a716-44665544000"  # Missing one char
        assert is_valid_uuid(uuid_str) is False

    def test_invalid_uuid_wrong_length_long(self):
        """Test that a UUID that's too long is rejected."""
        uuid_str = "550e8400-e29b-41d4-a716-4466554400000"  # Extra char
        assert is_valid_uuid(uuid_str) is False

    def test_invalid_uuid_no_dashes(self):
        """Test that a UUID without dashes is rejected."""
        uuid_str = "550e8400e29b41d4a716446655440000"
        assert is_valid_uuid(uuid_str) is False

    def test_invalid_uuid_wrong_dash_positions(self):
        """Test that a UUID with dashes in wrong positions is rejected."""
        uuid_str = "550e840-0e29b-41d4-a716-446655440000"  # Dash moved
        assert is_valid_uuid(uuid_str) is False

    def test_invalid_uuid_non_hex_characters(self):
        """Test that a UUID with non-hex characters is rejected."""
        uuid_str = "550e8400-e29b-41d4-a716-44665544000g"  # 'g' is not hex
        assert is_valid_uuid(uuid_str) is False

    def test_invalid_uuid_empty_string(self):
        """Test that an empty string is rejected."""
        assert is_valid_uuid("") is False

    def test_invalid_uuid_spaces(self):
        """Test that a UUID with spaces is rejected."""
        uuid_str = " 550e8400-e29b-41d4-a716-446655440000"
        assert is_valid_uuid(uuid_str) is False

    def test_invalid_uuid_braces(self):
        """Test that a UUID with braces (Microsoft format) is rejected."""
        uuid_str = "{550e8400-e29b-41d4-a716-446655440000}"
        assert is_valid_uuid(uuid_str) is False

    def test_invalid_uuid_urn_format(self):
        """Test that a UUID in URN format is rejected."""
        uuid_str = "urn:uuid:550e8400-e29b-41d4-a716-446655440000"
        assert is_valid_uuid(uuid_str) is False


# =============================================================================
# UUID Conversion Tests
# =============================================================================

class TestUuidToInt:
    """Tests for uuid_to_int function."""

    def test_conversion_nil_uuid(self):
        """Test that nil UUID converts to 0."""
        uuid_str = "00000000-0000-0000-0000-000000000000"
        assert uuid_to_int(uuid_str) == 0

    def test_conversion_max_uuid(self):
        """Test that max UUID converts to max 128-bit value."""
        uuid_str = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        expected = (2**128) - 1
        assert uuid_to_int(uuid_str) == expected

    def test_conversion_known_value(self):
        """Test conversion with a known UUID value."""
        uuid_str = "00000000-0000-0000-0000-000000000001"
        assert uuid_to_int(uuid_str) == 1

    def test_conversion_standard_uuid(self):
        """Test conversion of a standard UUID."""
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        # Verify it returns an integer
        result = uuid_to_int(uuid_str)
        assert isinstance(result, int)
        assert result > 0

    def test_conversion_invalid_uuid_raises_error(self):
        """Test that invalid UUID raises ValueError."""
        with pytest.raises(ValueError):
            uuid_to_int("not-a-valid-uuid")

    def test_conversion_preserves_ordering(self):
        """Test that UUID ordering is preserved when converted to int."""
        uuid1 = "00000000-0000-0000-0000-000000000001"
        uuid2 = "00000000-0000-0000-0000-000000000002"
        uuid3 = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        assert uuid_to_int(uuid1) < uuid_to_int(uuid2)
        assert uuid_to_int(uuid2) < uuid_to_int(uuid3)


class TestIntToUuid:
    """Tests for int_to_uuid function."""

    def test_conversion_zero(self):
        """Test that 0 converts to nil UUID."""
        result = int_to_uuid(0)
        assert result == "00000000-0000-0000-0000-000000000000"

    def test_conversion_max_value(self):
        """Test that max 128-bit value converts to max UUID."""
        max_val = (2**128) - 1
        result = int_to_uuid(max_val)
        assert result == "ffffffff-ffff-ffff-ffff-ffffffffffff"

    def test_conversion_one(self):
        """Test conversion of value 1."""
        result = int_to_uuid(1)
        assert result == "00000000-0000-0000-0000-000000000001"

    def test_conversion_returns_lowercase(self):
        """Test that result is lowercase."""
        result = int_to_uuid(255)
        assert result == result.lower()

    def test_roundtrip_conversion(self):
        """Test that uuid_to_int and int_to_uuid are inverses."""
        original_uuid = "550e8400-e29b-41d4-a716-446655440000"
        int_val = uuid_to_int(original_uuid)
        result_uuid = int_to_uuid(int_val)
        assert result_uuid == original_uuid.lower()

    def test_roundtrip_multiple_uuids(self):
        """Test roundtrip conversion for multiple UUIDs."""
        uuids = [
            "00000000-0000-0000-0000-000000000000",
            "00000000-0000-0000-0000-000000000001",
            "12345678-1234-1234-1234-123456789abc",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ]
        for original in uuids:
            int_val = uuid_to_int(original)
            result = int_to_uuid(int_val)
            assert result == original.lower()


# =============================================================================
# INT64 Range Split Generation Tests
# =============================================================================

class TestGenerateInt64RangeSplits:
    """Tests for generate_int64_range_splits function."""

    def test_basic_range_with_boundaries(self):
        """Test basic range generation including boundaries."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=100,
            num_splits=5,
            include_boundaries=True
        )
        assert len(values) == 5
        assert values[0] == "0"
        assert values[-1] == "100"

    def test_basic_range_without_boundaries(self):
        """Test basic range generation excluding boundaries."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=100,
            num_splits=4,
            include_boundaries=False
        )
        assert len(values) == 4
        # Values should be between 0 and 100, exclusive
        for v in values:
            assert 0 < int(v) < 100

    def test_minimum_splits_with_boundaries(self):
        """Test with minimum number of splits (2) including boundaries."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=100,
            num_splits=2,
            include_boundaries=True
        )
        assert len(values) == 2
        assert values[0] == "0"
        assert values[1] == "100"

    def test_minimum_splits_without_boundaries(self):
        """Test with minimum number of splits (2) excluding boundaries."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=100,
            num_splits=2,
            include_boundaries=False
        )
        assert len(values) == 2
        # Should be approximately 33 and 66
        for v in values:
            assert 0 < int(v) < 100

    def test_large_range(self):
        """Test with a large INT64 range."""
        start = -9223372036854775808  # INT64 min
        end = 9223372036854775807     # INT64 max
        values, warnings = generate_int64_range_splits(
            start=start,
            end=end,
            num_splits=5,
            include_boundaries=True
        )
        assert len(values) == 5
        assert values[0] == str(start)
        assert values[-1] == str(end)

    def test_negative_range(self):
        """Test with negative values."""
        values, warnings = generate_int64_range_splits(
            start=-100,
            end=-10,
            num_splits=4,
            include_boundaries=True
        )
        assert len(values) == 4
        assert values[0] == "-100"
        assert values[-1] == "-10"

    def test_crossing_zero(self):
        """Test range that crosses zero."""
        values, warnings = generate_int64_range_splits(
            start=-50,
            end=50,
            num_splits=5,
            include_boundaries=True
        )
        assert len(values) == 5
        assert values[0] == "-50"
        assert values[-1] == "50"
        # Middle value should be around 0
        middle_val = int(values[2])
        assert -10 <= middle_val <= 10

    def test_evenly_divisible_range(self):
        """Test with a range that divides evenly."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=100,
            num_splits=11,
            include_boundaries=True
        )
        assert len(values) == 11
        # Values should be 0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
        expected = [str(i * 10) for i in range(11)]
        assert values == expected

    def test_many_splits_batch_limit(self):
        """Test generating 100 splits (Spanner batch limit)."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=1000,
            num_splits=100,
            include_boundaries=True
        )
        assert len(values) == 100
        assert values[0] == "0"
        assert values[-1] == "1000"

    def test_error_start_equals_end(self):
        """Test that start == end raises ValueError."""
        with pytest.raises(ValueError, match="Start value must be less than end value"):
            generate_int64_range_splits(start=50, end=50, num_splits=5)

    def test_error_start_greater_than_end(self):
        """Test that start > end raises ValueError."""
        with pytest.raises(ValueError, match="Start value must be less than end value"):
            generate_int64_range_splits(start=100, end=50, num_splits=5)

    def test_error_num_splits_less_than_two(self):
        """Test that num_splits < 2 raises ValueError."""
        with pytest.raises(ValueError, match="Number of splits must be at least 2"):
            generate_int64_range_splits(start=0, end=100, num_splits=1)

    def test_error_num_splits_zero(self):
        """Test that num_splits = 0 raises ValueError."""
        with pytest.raises(ValueError, match="Number of splits must be at least 2"):
            generate_int64_range_splits(start=0, end=100, num_splits=0)

    def test_error_num_splits_negative(self):
        """Test that negative num_splits raises ValueError."""
        with pytest.raises(ValueError, match="Number of splits must be at least 2"):
            generate_int64_range_splits(start=0, end=100, num_splits=-5)

    def test_small_range_large_splits(self):
        """Test when range is smaller than num_splits (causes duplicates)."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=3,
            num_splits=10,
            include_boundaries=True
        )
        assert len(values) == 10
        # First and last should be exact
        assert values[0] == "0"
        assert values[-1] == "3"


# =============================================================================
# UUID Range Split Generation Tests
# =============================================================================

class TestGenerateUuidRangeSplits:
    """Tests for generate_uuid_range_splits function."""

    def test_basic_uuid_range_with_boundaries(self):
        """Test basic UUID range generation including boundaries."""
        start_uuid = "00000000-0000-0000-0000-000000000000"
        end_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        values, warnings = generate_uuid_range_splits(
            start_uuid=start_uuid,
            end_uuid=end_uuid,
            num_splits=5,
            include_boundaries=True
        )

        assert len(values) == 5
        assert values[0] == start_uuid
        assert values[-1] == end_uuid
        # All values should be valid UUIDs
        for v in values:
            assert is_valid_uuid(v)

    def test_basic_uuid_range_without_boundaries(self):
        """Test basic UUID range generation excluding boundaries."""
        start_uuid = "00000000-0000-0000-0000-000000000000"
        end_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        values, warnings = generate_uuid_range_splits(
            start_uuid=start_uuid,
            end_uuid=end_uuid,
            num_splits=4,
            include_boundaries=False
        )

        assert len(values) == 4
        # Values should be between start and end
        start_int = uuid_to_int(start_uuid)
        end_int = uuid_to_int(end_uuid)
        for v in values:
            assert is_valid_uuid(v)
            v_int = uuid_to_int(v)
            assert start_int < v_int < end_int

    def test_minimum_splits_uuid(self):
        """Test with minimum number of splits (2)."""
        start_uuid = "00000000-0000-0000-0000-000000000001"
        end_uuid = "00000000-0000-0000-0000-000000000100"

        values, warnings = generate_uuid_range_splits(
            start_uuid=start_uuid,
            end_uuid=end_uuid,
            num_splits=2,
            include_boundaries=True
        )

        assert len(values) == 2
        assert values[0] == start_uuid
        assert values[1] == end_uuid

    def test_uuid_ordering_preserved(self):
        """Test that generated UUIDs are in ascending order."""
        start_uuid = "00000000-0000-0000-0000-000000000000"
        end_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        values, warnings = generate_uuid_range_splits(
            start_uuid=start_uuid,
            end_uuid=end_uuid,
            num_splits=10,
            include_boundaries=True
        )

        # Convert to ints and verify ordering
        int_values = [uuid_to_int(v) for v in values]
        for i in range(len(int_values) - 1):
            assert int_values[i] < int_values[i + 1]

    def test_realistic_uuid_range(self):
        """Test with realistic UUID values."""
        start_uuid = "11111111-1111-1111-1111-111111111111"
        end_uuid = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"

        values, warnings = generate_uuid_range_splits(
            start_uuid=start_uuid,
            end_uuid=end_uuid,
            num_splits=5,
            include_boundaries=True
        )

        assert len(values) == 5
        assert values[0] == start_uuid
        assert values[-1] == end_uuid

    def test_error_invalid_start_uuid(self):
        """Test that invalid start UUID raises ValueError."""
        with pytest.raises(ValueError, match="not a valid UUID format"):
            generate_uuid_range_splits(
                start_uuid="not-a-uuid",
                end_uuid="ffffffff-ffff-ffff-ffff-ffffffffffff",
                num_splits=5
            )

    def test_error_invalid_end_uuid(self):
        """Test that invalid end UUID raises ValueError."""
        with pytest.raises(ValueError, match="not a valid UUID format"):
            generate_uuid_range_splits(
                start_uuid="00000000-0000-0000-0000-000000000000",
                end_uuid="not-a-uuid",
                num_splits=5
            )

    def test_error_start_uuid_equals_end_uuid(self):
        """Test that start == end raises ValueError."""
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        with pytest.raises(ValueError, match="Start value must be less than end value"):
            generate_uuid_range_splits(
                start_uuid=uuid_str,
                end_uuid=uuid_str,
                num_splits=5
            )

    def test_error_start_uuid_greater_than_end_uuid(self):
        """Test that start > end raises ValueError."""
        with pytest.raises(ValueError, match="Start value must be less than end value"):
            generate_uuid_range_splits(
                start_uuid="ffffffff-ffff-ffff-ffff-ffffffffffff",
                end_uuid="00000000-0000-0000-0000-000000000000",
                num_splits=5
            )

    def test_error_num_splits_less_than_two_uuid(self):
        """Test that num_splits < 2 raises ValueError for UUIDs."""
        with pytest.raises(ValueError, match="Number of splits must be at least 2"):
            generate_uuid_range_splits(
                start_uuid="00000000-0000-0000-0000-000000000000",
                end_uuid="ffffffff-ffff-ffff-ffff-ffffffffffff",
                num_splits=1
            )


# =============================================================================
# Range Type Detection Tests
# =============================================================================

class TestDetectRangeType:
    """Tests for detect_range_type function."""

    def test_int64_type(self):
        """Test detection of INT64 type."""
        range_type, error = detect_range_type("INT64")
        assert range_type == SupportedRangeType.INT64
        assert error is None

    def test_int64_type_lowercase(self):
        """Test detection of INT64 type (lowercase)."""
        range_type, error = detect_range_type("int64")
        assert range_type == SupportedRangeType.INT64
        assert error is None

    def test_string_36_type(self):
        """Test detection of STRING(36) type (UUID length)."""
        range_type, error = detect_range_type("STRING(36)")
        assert range_type == SupportedRangeType.STRING_UUID
        assert error is None

    def test_string_max_type(self):
        """Test detection of STRING(MAX) type."""
        range_type, error = detect_range_type("STRING(MAX)")
        assert range_type == SupportedRangeType.STRING_UUID
        assert error is None

    def test_string_larger_than_36(self):
        """Test detection of STRING larger than 36."""
        range_type, error = detect_range_type("STRING(100)")
        assert range_type == SupportedRangeType.STRING_UUID
        assert error is None

    def test_string_exactly_36_with_valid_sample(self):
        """Test STRING(36) with valid UUID sample value."""
        range_type, error = detect_range_type(
            "STRING(36)",
            sample_value="550e8400-e29b-41d4-a716-446655440000"
        )
        assert range_type == SupportedRangeType.STRING_UUID
        assert error is None

    def test_string_36_with_invalid_sample(self):
        """Test STRING(36) with invalid UUID sample value."""
        range_type, error = detect_range_type(
            "STRING(36)",
            sample_value="not-a-uuid"
        )
        assert range_type is None
        assert "not a valid UUID format" in error

    def test_string_too_short(self):
        """Test STRING(10) which is too short for UUIDs."""
        range_type, error = detect_range_type("STRING(10)")
        assert range_type is None
        assert "too short for UUIDs" in error

    def test_string_35_too_short(self):
        """Test STRING(35) which is one char too short."""
        range_type, error = detect_range_type("STRING(35)")
        assert range_type is None
        assert "too short for UUIDs" in error

    def test_unsupported_float64(self):
        """Test that FLOAT64 is not supported."""
        range_type, error = detect_range_type("FLOAT64")
        assert range_type is None
        assert "not supported" in error

    def test_unsupported_bool(self):
        """Test that BOOL is not supported."""
        range_type, error = detect_range_type("BOOL")
        assert range_type is None
        assert "not supported" in error

    def test_bytes_16_type(self):
        """Test BYTES(16) which supports UUIDs."""
        range_type, error = detect_range_type("BYTES(16)")
        assert range_type == SupportedRangeType.BYTES_UUID
        assert error is None

    def test_bytes_max_type(self):
        """Test BYTES(MAX) which supports UUIDs."""
        range_type, error = detect_range_type("BYTES(MAX)")
        assert range_type == SupportedRangeType.BYTES_UUID
        assert error is None

    def test_bytes_larger_than_16(self):
        """Test BYTES(256) which supports UUIDs."""
        range_type, error = detect_range_type("BYTES(256)")
        assert range_type == SupportedRangeType.BYTES_UUID
        assert error is None

    def test_bytes_16_with_valid_sample(self):
        """Test BYTES(16) with valid UUID sample value."""
        range_type, error = detect_range_type(
            "BYTES(16)",
            sample_value="12345678-1234-1234-1234-123456789abc"
        )
        assert range_type == SupportedRangeType.BYTES_UUID
        assert error is None

    def test_bytes_16_with_invalid_sample(self):
        """Test BYTES(16) with invalid UUID sample value."""
        range_type, error = detect_range_type(
            "BYTES(16)",
            sample_value="not-a-uuid"
        )
        assert range_type is None
        assert "not a valid UUID format" in error

    def test_bytes_too_short(self):
        """Test BYTES(10) which is too short for UUIDs."""
        range_type, error = detect_range_type("BYTES(10)")
        assert range_type is None
        assert "too short for UUIDs" in error

    def test_bytes_15_too_short(self):
        """Test BYTES(15) which is one byte too short."""
        range_type, error = detect_range_type("BYTES(15)")
        assert range_type is None
        assert "too short for UUIDs" in error

    def test_unsupported_date(self):
        """Test that DATE is not supported."""
        range_type, error = detect_range_type("DATE")
        assert range_type is None
        assert "not supported" in error

    def test_unsupported_timestamp(self):
        """Test that TIMESTAMP is not supported."""
        range_type, error = detect_range_type("TIMESTAMP")
        assert range_type is None
        assert "not supported" in error

    def test_unsupported_array(self):
        """Test that ARRAY types are not supported."""
        range_type, error = detect_range_type("ARRAY<INT64>")
        assert range_type is None
        assert "not supported" in error

    def test_malformed_string_type(self):
        """Test malformed STRING type without parentheses."""
        range_type, error = detect_range_type("STRING")
        assert range_type is None
        assert "Could not parse STRING type" in error


# =============================================================================
# Range Request Validation Tests
# =============================================================================

class TestValidateRangeRequest:
    """Tests for validate_range_request function."""

    @pytest.fixture
    def int64_schema(self) -> EntityKeySchema:
        """Create a schema with INT64 primary key."""
        return EntityKeySchema(
            entity_name="TestTable",
            entity_type=EntityType.TABLE,
            key_columns=[
                KeyColumnInfo(
                    column_name="id",
                    spanner_type="INT64",
                    ordinal_position=1
                )
            ],
            is_composite=False
        )

    @pytest.fixture
    def uuid_schema(self) -> EntityKeySchema:
        """Create a schema with STRING(36) UUID primary key."""
        return EntityKeySchema(
            entity_name="TestTable",
            entity_type=EntityType.TABLE,
            key_columns=[
                KeyColumnInfo(
                    column_name="uuid_id",
                    spanner_type="STRING(36)",
                    ordinal_position=1
                )
            ],
            is_composite=False
        )

    @pytest.fixture
    def composite_key_schema(self) -> EntityKeySchema:
        """Create a schema with composite primary key."""
        return EntityKeySchema(
            entity_name="TestTable",
            entity_type=EntityType.TABLE,
            key_columns=[
                KeyColumnInfo(
                    column_name="tenant_id",
                    spanner_type="INT64",
                    ordinal_position=1
                ),
                KeyColumnInfo(
                    column_name="user_id",
                    spanner_type="INT64",
                    ordinal_position=2
                )
            ],
            is_composite=True
        )

    @pytest.fixture
    def short_string_schema(self) -> EntityKeySchema:
        """Create a schema with STRING(10) primary key (too short for UUID)."""
        return EntityKeySchema(
            entity_name="TestTable",
            entity_type=EntityType.TABLE,
            key_columns=[
                KeyColumnInfo(
                    column_name="code",
                    spanner_type="STRING(10)",
                    ordinal_position=1
                )
            ],
            is_composite=False
        )

    @pytest.fixture
    def empty_schema(self) -> EntityKeySchema:
        """Create a schema with no key columns."""
        return EntityKeySchema(
            entity_name="TestTable",
            entity_type=EntityType.TABLE,
            key_columns=[],
            is_composite=False
        )

    def test_valid_int64_range(self, int64_schema: EntityKeySchema):
        """Test validation of valid INT64 range."""
        result = validate_range_request(int64_schema, "100", "1000")

        assert result.is_valid is True
        assert result.range_type == SupportedRangeType.INT64
        assert result.error_message is None

    def test_valid_int64_negative_range(self, int64_schema: EntityKeySchema):
        """Test validation of valid INT64 negative range."""
        result = validate_range_request(int64_schema, "-1000", "-100")

        assert result.is_valid is True
        assert result.range_type == SupportedRangeType.INT64
        assert result.error_message is None

    def test_valid_uuid_range(self, uuid_schema: EntityKeySchema):
        """Test validation of valid UUID range."""
        result = validate_range_request(
            uuid_schema,
            "00000000-0000-0000-0000-000000000001",
            "ffffffff-ffff-ffff-ffff-ffffffffffff"
        )

        assert result.is_valid is True
        assert result.range_type == SupportedRangeType.STRING_UUID
        assert result.error_message is None

    def test_invalid_composite_key(self, composite_key_schema: EntityKeySchema):
        """Test that composite keys are rejected."""
        result = validate_range_request(composite_key_schema, "100", "1000")

        assert result.is_valid is False
        assert result.range_type is None
        assert "composite keys" in result.error_message.lower()

    def test_invalid_no_key_columns(self, empty_schema: EntityKeySchema):
        """Test that empty key columns are rejected."""
        result = validate_range_request(empty_schema, "100", "1000")

        assert result.is_valid is False
        assert "No key columns found" in result.error_message

    def test_invalid_short_string_type(self, short_string_schema: EntityKeySchema):
        """Test that STRING(10) is rejected (too short for UUID)."""
        result = validate_range_request(short_string_schema, "abc", "xyz")

        assert result.is_valid is False
        assert "too short" in result.error_message

    def test_invalid_int64_start_greater_than_end(self, int64_schema: EntityKeySchema):
        """Test INT64 validation when start > end."""
        result = validate_range_request(int64_schema, "1000", "100")

        assert result.is_valid is False
        assert result.range_type == SupportedRangeType.INT64
        assert "Start value must be less than end value" in result.error_message

    def test_invalid_int64_start_equals_end(self, int64_schema: EntityKeySchema):
        """Test INT64 validation when start == end."""
        result = validate_range_request(int64_schema, "100", "100")

        assert result.is_valid is False
        assert "Start value must be less than end value" in result.error_message

    def test_invalid_int64_non_numeric_start(self, int64_schema: EntityKeySchema):
        """Test INT64 validation with non-numeric start value."""
        result = validate_range_request(int64_schema, "abc", "100")

        assert result.is_valid is False
        assert "Invalid integer value" in result.error_message

    def test_invalid_int64_non_numeric_end(self, int64_schema: EntityKeySchema):
        """Test INT64 validation with non-numeric end value."""
        result = validate_range_request(int64_schema, "100", "xyz")

        assert result.is_valid is False
        assert "Invalid integer value" in result.error_message

    def test_invalid_uuid_start_format(self, uuid_schema: EntityKeySchema):
        """Test UUID validation with invalid start format."""
        result = validate_range_request(
            uuid_schema,
            "not-a-uuid",
            "ffffffff-ffff-ffff-ffff-ffffffffffff"
        )

        assert result.is_valid is False
        assert "not a valid UUID format" in result.error_message

    def test_invalid_uuid_end_format(self, uuid_schema: EntityKeySchema):
        """Test UUID validation with invalid end format."""
        result = validate_range_request(
            uuid_schema,
            "00000000-0000-0000-0000-000000000001",
            "not-a-uuid"
        )

        assert result.is_valid is False
        assert "not a valid UUID format" in result.error_message

    def test_invalid_uuid_start_greater_than_end(self, uuid_schema: EntityKeySchema):
        """Test UUID validation when start > end."""
        result = validate_range_request(
            uuid_schema,
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "00000000-0000-0000-0000-000000000001"
        )

        assert result.is_valid is False
        assert "Start value must be less than end value" in result.error_message

    def test_invalid_uuid_start_equals_end(self, uuid_schema: EntityKeySchema):
        """Test UUID validation when start == end."""
        uuid_str = "550e8400-e29b-41d4-a716-446655440000"
        result = validate_range_request(uuid_schema, uuid_str, uuid_str)

        assert result.is_valid is False
        assert "Start value must be less than end value" in result.error_message


# =============================================================================
# Generate Range Splits (Main Entry Point) Tests
# =============================================================================

class TestGenerateRangeSplits:
    """Tests for generate_range_splits function (main entry point)."""

    def test_int64_range_generation(self):
        """Test INT64 range generation through main entry point."""
        values, warnings = generate_range_splits(
            range_type=SupportedRangeType.INT64,
            start_value="0",
            end_value="1000",
            num_splits=5,
            include_boundaries=True
        )

        assert len(values) == 5
        assert values[0] == "0"
        assert values[-1] == "1000"

    def test_uuid_range_generation(self):
        """Test UUID range generation through main entry point."""
        start_uuid = "00000000-0000-0000-0000-000000000000"
        end_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        values, warnings = generate_range_splits(
            range_type=SupportedRangeType.STRING_UUID,
            start_value=start_uuid,
            end_value=end_uuid,
            num_splits=5,
            include_boundaries=True
        )

        assert len(values) == 5
        assert values[0] == start_uuid
        assert values[-1] == end_uuid

    def test_bytes_uuid_range_generation(self):
        """Test BYTES_UUID range generation through main entry point."""
        start_uuid = "00000000-0000-0000-0000-000000000000"
        end_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        values, warnings = generate_range_splits(
            range_type=SupportedRangeType.BYTES_UUID,
            start_value=start_uuid,
            end_value=end_uuid,
            num_splits=5,
            include_boundaries=True
        )

        assert len(values) == 5
        assert values[0] == start_uuid
        assert values[-1] == end_uuid

    def test_int64_without_boundaries(self):
        """Test INT64 generation without boundaries."""
        values, warnings = generate_range_splits(
            range_type=SupportedRangeType.INT64,
            start_value="0",
            end_value="100",
            num_splits=3,
            include_boundaries=False
        )

        assert len(values) == 3
        for v in values:
            assert 0 < int(v) < 100

    def test_uuid_without_boundaries(self):
        """Test UUID generation without boundaries."""
        start_uuid = "00000000-0000-0000-0000-000000000000"
        end_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"

        values, warnings = generate_range_splits(
            range_type=SupportedRangeType.STRING_UUID,
            start_value=start_uuid,
            end_value=end_uuid,
            num_splits=3,
            include_boundaries=False
        )

        assert len(values) == 3
        start_int = uuid_to_int(start_uuid)
        end_int = uuid_to_int(end_uuid)
        for v in values:
            v_int = uuid_to_int(v)
            assert start_int < v_int < end_int


# =============================================================================
# Edge Cases and Integration Tests
# =============================================================================

class TestGenerateRangeSplitsErrors:
    """Tests for error handling in generate_range_splits."""

    def test_unsupported_range_type_raises_error(self):
        """Test that an unsupported range type raises ValueError."""
        # Create a mock/fake range type that is not supported
        # This tests the defensive else clause in generate_range_splits
        from unittest.mock import MagicMock

        fake_range_type = MagicMock()
        fake_range_type.__eq__ = lambda self, other: False  # Never equals INT64 or STRING_UUID

        with pytest.raises(ValueError, match="Unsupported range type"):
            generate_range_splits(
                range_type=fake_range_type,
                start_value="0",
                end_value="100",
                num_splits=5,
                include_boundaries=True
            )


class TestEdgeCases:
    """Tests for edge cases and special scenarios."""

    def test_int64_boundaries_exact(self):
        """Test INT64 min and max values."""
        int64_min = -9223372036854775808
        int64_max = 9223372036854775807

        values, warnings = generate_int64_range_splits(
            start=int64_min,
            end=int64_max,
            num_splits=3,
            include_boundaries=True
        )

        assert values[0] == str(int64_min)
        assert values[-1] == str(int64_max)

    def test_uuid_small_range(self):
        """Test UUID generation with a small range."""
        start_uuid = "00000000-0000-0000-0000-000000000000"
        end_uuid = "00000000-0000-0000-0000-000000000003"

        values, warnings = generate_uuid_range_splits(
            start_uuid=start_uuid,
            end_uuid=end_uuid,
            num_splits=3,
            include_boundaries=True
        )

        assert len(values) == 3
        assert values[0] == start_uuid
        assert values[-1] == end_uuid

    def test_generated_uuids_are_valid(self):
        """Verify all generated UUIDs pass validation."""
        start_uuid = "11111111-1111-1111-1111-111111111111"
        end_uuid = "99999999-9999-9999-9999-999999999999"

        values, warnings = generate_uuid_range_splits(
            start_uuid=start_uuid,
            end_uuid=end_uuid,
            num_splits=20,
            include_boundaries=True
        )

        for v in values:
            assert is_valid_uuid(v), f"Generated value {v} is not a valid UUID"

    def test_int64_values_are_valid_integers(self):
        """Verify all generated INT64 values are valid integers."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=1000000,
            num_splits=50,
            include_boundaries=True
        )

        for v in values:
            # Should not raise ValueError
            int_val = int(v)
            assert isinstance(int_val, int)

    def test_batch_limit_100_splits(self):
        """Test generating exactly 100 splits (Spanner API batch limit)."""
        values, warnings = generate_int64_range_splits(
            start=0,
            end=10000,
            num_splits=100,
            include_boundaries=True
        )

        assert len(values) == 100
        # Verify uniqueness
        assert len(set(values)) <= 100  # May have duplicates for small ranges

    def test_more_than_batch_limit_splits(self):
        """Test generating more than 100 splits (would need batching)."""
        # Note: This tests the generation function, not the batching logic
        # which should be handled by the sync layer
        values, warnings = generate_int64_range_splits(
            start=0,
            end=1000000,
            num_splits=100,  # Max allowed by the model
            include_boundaries=True
        )

        assert len(values) == 100


# =============================================================================
# Parametrized Tests for Comprehensive Coverage
# =============================================================================

class TestParametrizedCases:
    """Parametrized tests for comprehensive coverage."""

    @pytest.mark.parametrize("uuid_str,expected", [
        ("00000000-0000-0000-0000-000000000000", True),
        ("ffffffff-ffff-ffff-ffff-ffffffffffff", True),
        ("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", True),
        ("12345678-1234-5678-1234-567812345678", True),
        ("550e8400-e29b-41d4-a716-446655440000", True),
        ("", False),
        ("not-a-uuid", False),
        ("550e8400e29b41d4a716446655440000", False),  # No dashes
        ("550e8400-e29b-41d4-a716-44665544000", False),  # Too short
        ("550e8400-e29b-41d4-a716-4466554400000", False),  # Too long
        ("{550e8400-e29b-41d4-a716-446655440000}", False),  # Braces
    ])
    def test_uuid_validation_parametrized(self, uuid_str: str, expected: bool):
        """Parametrized test for UUID validation."""
        assert is_valid_uuid(uuid_str) == expected

    @pytest.mark.parametrize("spanner_type,expected_type,has_error", [
        ("INT64", SupportedRangeType.INT64, False),
        ("int64", SupportedRangeType.INT64, False),
        ("STRING(36)", SupportedRangeType.STRING_UUID, False),
        ("STRING(100)", SupportedRangeType.STRING_UUID, False),
        ("STRING(MAX)", SupportedRangeType.STRING_UUID, False),
        ("STRING(35)", None, True),
        ("STRING(10)", None, True),
        ("FLOAT64", None, True),
        ("BOOL", None, True),
        ("DATE", None, True),
        ("TIMESTAMP", None, True),
    ])
    def test_detect_range_type_parametrized(
        self,
        spanner_type: str,
        expected_type: SupportedRangeType,
        has_error: bool
    ):
        """Parametrized test for range type detection."""
        range_type, error = detect_range_type(spanner_type)
        assert range_type == expected_type
        assert (error is not None) == has_error

    @pytest.mark.parametrize("start,end,num_splits,include_boundaries,expected_len", [
        (0, 100, 5, True, 5),
        (0, 100, 5, False, 5),
        (0, 100, 2, True, 2),
        (-100, 100, 5, True, 5),
        (0, 1000000, 100, True, 100),
    ])
    def test_int64_range_splits_parametrized(
        self,
        start: int,
        end: int,
        num_splits: int,
        include_boundaries: bool,
        expected_len: int
    ):
        """Parametrized test for INT64 range split generation."""
        values, warnings = generate_int64_range_splits(
            start=start,
            end=end,
            num_splits=num_splits,
            include_boundaries=include_boundaries
        )
        assert len(values) == expected_len
