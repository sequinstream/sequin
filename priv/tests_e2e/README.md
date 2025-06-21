# End-to-End Tests

This directory contains the end-to-end tests for Sequin, testing the integration between PostgreSQL and multiple destinations including Kafka and Amazon SQS using demo table setups.

## Test Setup

The test environment consists of:
- PostgreSQL database with test tables (port 5412)
- Kafka broker for message streaming (port 9012)
- LocalStack for SQS emulation (port 4566)
- Redis for caching (port 6319)
- Sequin service configured to stream changes (port 7316)

## Configuration

Environment variables:
- `TEST_MESSAGES_COUNT`: Number of test messages to generate and verify (default: 1000)

Example configurations:
```bash
# Run all tests with default 1000 messages
make e2e-tests

# Run specific integration tests
make e2e-test tag=kafka  # Only Kafka tests
make e2e-test tag=sqs    # Only SQS tests

# Run with custom message count
TEST_MESSAGES_COUNT=500 make e2e-tests

# Run specific tests with custom message count
TEST_MESSAGES_COUNT=500 make e2e-test tag=kafka

# Run with larger dataset
TEST_MESSAGES_COUNT=10000 make e2e-tests
```

## Directory Structure

```
priv/tests_e2e/
├── docker-compose.yml    # Docker services configuration
├── postgres/
│   └── 00-setup.sql     # Database initialization script
├── sequin.yml           # Sequin configuration
└── tests.exs            # Test implementation
```

## Available Make Commands

From the root directory, you can use these commands to manage the test environment:

```bash
make e2e-up        # Start all containers
make e2e-down      # Stop and remove containers and volumes
make e2e-rebuild   # Rebuild containers without cache
make e2e-restart   # Restart all containers (down + up)
make e2e-tests     # Run all e2e tests
make e2e-test tag=kafka  # Run only Kafka integration tests
make e2e-test tag=sqs    # Run only SQS integration tests
make e2e-clean     # Clean up containers
```

## Test Flow

1. The test environment sets up two test tables:
   - `kafka_test_table` for Kafka streaming tests
   - `sqs_test_table` for Amazon SQS streaming tests
   
   Each table contains:
   - Auto-incrementing ID
   - Text field
   - Created timestamp

2. For each destination (Kafka and SQS):
   - Inserts the specified number of test records (controlled by TEST_MESSAGES_COUNT)
   - Verifies that all changes are properly streamed to the respective destination
   - Validates message contents and structure

## Running Tests

To run the complete test suite:

1. Start the environment:
   ```bash
   make e2e-up
   ```

2. Run the tests (with optional configuration):
   ```bash
   # Run all tests
   make e2e-tests

   # Run specific integration tests
   make e2e-test tag=kafka  # Only Kafka tests
   make e2e-test tag=sqs    # Only SQS tests

   # Custom message count
   TEST_MESSAGES_COUNT=500 make e2e-tests          # All tests
   TEST_MESSAGES_COUNT=500 make e2e-test tag=kafka # Only Kafka tests
   ```

3. Clean up after:
   ```bash
   make e2e-down
   ```

## Debugging

If you need to rebuild the environment from scratch:
```bash
make e2e-rebuild
```

To view logs:
```bash
# From project root
make e2e-logs

# Or manually
cd priv/tests_e2e && docker compose logs -f
```

## Performance Testing

You can use the `TEST_MESSAGES_COUNT` environment variable to perform basic performance testing:

```bash
# Test with small dataset
TEST_MESSAGES_COUNT=100 make e2e-test

# Test with medium dataset
TEST_MESSAGES_COUNT=1000 make e2e-test

# Test with large dataset
TEST_MESSAGES_COUNT=10000 make e2e-test
```

This allows you to verify Sequin's performance with different data volumes and ensure it handles larger datasets correctly across both Kafka and SQS destinations. 