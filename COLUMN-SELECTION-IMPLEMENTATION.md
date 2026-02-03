# Column Selection Implementation

## Overview

This implementation adds the ability to filter columns from WAL pipeline CDC events, similar to Debezium's column filtering feature. This allows users to exclude sensitive columns (like passwords, SSNs, etc.) or include only specific columns they need.

## Core Functionality

### Schema Changes

**File**: `lib/sequin/replication/source_table/source_table.ex`

Added two new fields to `SourceTable`:
- `include_column_attnums` - List of column attribute numbers to include (whitelist)
- `exclude_column_attnums` - List of column attribute numbers to exclude (blacklist)

These fields are mutually exclusive - only one can be set at a time.

### Filtering Logic

**File**: `lib/sequin/replication/source_table/column_selection.ex`

Created a new module with helper functions:
- `filter_fields/2` - Filters `Message.Field` structs based on column selection
- `filter_column_attnums/2` - Filters column attribute numbers (used during backfills)
- `has_column_selection?/1` - Returns true if column selection is configured

### Integration Points

1. **WAL Event Creation** (`lib/sequin/runtime/message_handler.ex`)
   - Modified `wal_event/2` to pass the source_table configuration to filtering functions
   - Filters columns when creating `record` and `changes` fields

2. **Message Transformation** (`lib/sequin/consumers/consumers.ex`)
   - Updated `message_record/2` to accept optional `source_table` parameter
   - Updated `message_changes/2` to filter both old_fields and new_fields
   - Added `maybe_filter_columns/2` helper function

3. **Database Migration** (`priv/repo/migrations/20260203141912_add_column_selection_to_source_tables.exs`)
   - Adds the new fields to existing WAL pipelines with NULL defaults

## Usage (To Be Implemented)

Column selection will be configured through:

1. **YAML Configuration**:
```yaml
change_retentions:
  - name: "users-pipeline"
    source_table_name: "users"
    exclude_columns:
      - "password_hash"
      - "ssn"
```

2. **Web UI**: Column selection checkboxes in the WAL Pipeline form

3. **API**: Through the management API when creating/updating WAL pipelines

## Testing

Three test files were created:

1. `test/sequin/replication/source_table/column_selection_test.exs` - Tests the filtering logic
2. `test/sequin/replication/source_table/source_table_test.exs` - Tests schema validation
3. `test/sequin/consumers/column_filtering_test.exs` - Tests message transformation

All tests pass and verify:
- Column inclusion/exclusion filtering
- Mutually exclusive validation
- Integration with message records and changes
- Handling of nil/empty configurations

## Next Steps

The core filtering functionality is complete. To make it user-accessible:

1. Add UI components for column selection in WAL Pipeline forms
2. Add YAML parsing logic to convert column names to attnums
3. Add API endpoints for configuring column selection
4. Update documentation and examples
