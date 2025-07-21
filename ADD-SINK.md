# Adding a New Sink to Sequin

This document outlines the steps required to add a new sink to Sequin. A sink is a destination where Sequin can send data changes.

## Steps:

### [Backend] Add the sink schema

Create a new sink schema in `lib/sequin/consumers/` (e.g., `my_sink.ex`). The schema should:
- Use `Ecto.Schema` and `TypedEctoSchema`
- Define required fields and their types
- Implement validation in a changeset function

Example: `lib/sequin/consumers/kafka_sink.ex`

### [Backend] Add the sink type to SinkConsumer

Update `lib/sequin/consumers/sink_consumer.ex`:
- Add the new sink type to the `@sink_types` list
- Add the new module to the `sink_module/1` mapping

### [Backend] Create the sink client

Create a new client in `lib/sequin/sinks/` (e.g., `my_sink/client.ex`). The client should:
- Handle API communication with the sink service. Use Req if the sink uses HTTP. Otherwise we may need to bring in a library like AWS or :brod.
- Implement `test_connection/1` for connection validation
- Implement methods for sending data (e.g., `append_records/2`)
- Handle error cases and logging appropriately
- If using Req, we can support testing with req_opts. If not, we need a client behavior and for the client to implement that behavior.

For HTTP see: lib/sequin/sinks/typesense/client.ex
For non-http see: lib/sequin/sinks/kafka/kafka.ex and lib/sequin/sinks/kafka/client.ex

### [Backend] Add the sink pipeline

Create a new pipeline in `lib/sequin/runtime/` (e.g., `my_sink_pipeline.ex`). The pipeline should:
- Implement the `Sequin.Runtime.SinkPipeline` behaviour
- Define batching configuration
- Handle message transformation and delivery
- Implement error handling and retries

Example: `lib/sequin/runtime/kafka_pipeline.ex`


### [Backend] Update the pipeline registry

Add the new sink type to `lib/sequin/runtime/sink_pipeline.ex` in the `pipeline_module/1` function.

### [Backend] Add transforms support

Update `lib/sequin/transforms/transforms.ex`:
- Add `to_external/2` function for the new sink type
- Add parsing support in `parse_sink/2`

### [Backend] Update configuration

Update relevant config files (e.g., `config/test.exs`) to add any necessary configuration for the new sink.

### [Frontend] Add sink type to TypeScript types

Update `assets/svelte/consumers/types.ts`:
- Add new sink type interface
- Update the Consumer union type

### [Frontend] Create sink components

Create new components in `assets/svelte/sinks/my_sink/`:
- `MySinkIcon.svelte` - Sink icon component
- `MySinkSinkCard.svelte` - Display component for sink details
- `MySinkSinkForm.svelte` - Form component for sink configuration

### [Frontend] Update consumer components

Update the following components to include the new sink:
- `assets/svelte/consumers/ShowSink.svelte`
- `assets/svelte/consumers/ShowSinkHeader.svelte`
- `assets/svelte/consumers/SinkConsumerForm.svelte`
- `assets/svelte/consumers/SinkIndex.svelte`

### [Frontend] Update consumer form handler

Update `lib/sequin_web/live/components/consumer_form.ex`:
- Add sink-specific connection testing
- Add encoding/decoding functions
- Update the sink title helper

### [Frontend] Update live view handlers

Update relevant live view handlers in `lib/sequin_web/live/`:
- Add sink-specific handling in show.ex
- Update any relevant forms or displays


### [Tests] Update existing tests

Update:
- Factory modules in `test/support/factory/`
- YAML loader tests
- Consumer form tests

### [Tests] Add test coverage

Create a minimal pipeline test with a mock client or req adapter. See:
- test/sequin/kafka_pipeline_test.exs OR
- test/sequin/typesense_pipeline_test.exs

Also create tests for:
- Sink schema and changeset validation
- Client functionality and error handling

### [DOCS] Add reference, how-to, and quickstart docs

See:
- docs/reference/sinks/kafka.mdx
- docs/how-to/stream-postgres-to-kafka.mdx
- docs/quickstart/kafka.mdx