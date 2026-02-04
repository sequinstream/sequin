# Add routing to sink consumer

We are adding routing functionality to sink consumers, one at a time. Along the way we consider ways to make the process easier for each additional sink.

## Steps:

### [Design] Determine what is routable

For the sink, determine which keys are routable.

Sinks can process records from many tables. They process inserts, updates, deletes, and reads (from backfills).

So Kafka topics are routable but not the Kafka host. Redis String keys are routable but not the redis database number. HTTP method, path, and headers are routable but not the HTTP host.

### [Plan] Review the commit of the last routing we added

Run the following git command to view relevant files and changes:

```
git show 3724ffc1b6cb8763747355316051c504a4808f81
```

### [Backend] Modify changeset

Based on which keys are routable, we may want to modify the sink changeset to enforce certain keys being required or nil etc. based on routing_mode.

For instance Kafka (`lib/sequin/consumers/kafka_sink.ex`) implements `validate_routing/1` to enforce that the `topic` key is required when routing_mode is `static` and to set `topic` to `nil` when routing_mode is `dynamic`.

Tests may have to be updated, and at least one new test should be included for the changeset validation. See `test/sequin/kafka_sink_test.exs` for an example.

### [Backend] Add the sink type to the routing function

The RoutingFunction schema requires a `sink_type` field (`lib/sequin/consumers/routing_function.ex`).

Update the function factory in `test/support/factory/functions_factory.ex` to include the new sink type.

### [Backend] Add RoutedConsumer

Behaviour / macro: `lib/sequin/runtime/routing/routed_consumer.ex`

The `route/4` callback defines the system defaults both for static and dynamically routed instances of the sink.

The `route_consumer/1` callback defines the consumer-specific overrides when statically routed.

The user defines the routing function which implements overrides when dynamically routed.

Example implementation: `lib/sequin/runtime/routing/consumers/kafka.ex`

### [Backend] Update the pipeline

Each sink type implements a behavior for the delivery pipeline. This needs to be updated to support routing.

Typically two parts are updated:

1. handle_message/2 requires updates to batching. this is where we perform routing and assign the routing info into the batch key
2. handle_batch/2 requires updates to delivery. this is where we account for routing info in how we push messages to the sink destination.

See `lib/sequin/runtime/kafka_pipeline.ex` for an example.

This is the most complicated part. We also likely need to touch the client (ie. `lib/sequin/sinks/kafka/client.ex`).

Tests may have to be created or updated (ie. `lib/sequin/runtime/kafka_pipeline.ex`).

### [Frontend] Include the default routing function in LiveView

The default routing function is defined in `lib/sequin_web/live/functions/edit.ex` per sink type.

It must be added to `@initial_code_map` as well.

### [Frontend] Add the sink type

The `sinkTypeInternalToExternal` map in `assets/svelte/functions/Edit.svelte` needs to be updated to include the new sink type.

The `RoutedSinkTypeValues` list in `assets/svelte/consumers/types.ts` needs to be updated to include the new sink type.

A new value in `sinkTypeInternalToExternal` needs to be added so it appears in the functions edit form sink type dropdown in (`assets/svelte/functions/Edit.svelte`).

### [Frontend] Update the sink consumer form

Sink consumer forms typically have a <Card> for the sink configuration. We should remove the fields from this card that are routable and add them to a new <Card> for the routing configuration.

To support this we need `let isDynamicRouting = form.routingMode === "dynamic";` and function related exports:

```ts
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
```

See `assets/svelte/sinks/kafka/KafkaSinkForm.svelte` for an example.

We also need to thread new props to this sink form from `assets/svelte/consumers/SinkConsumerForm.svelte`.

### [Frontend] Add the dynamic routing docs

In the last step we implemented `<DynamicRoutingForm>` which is powered by content in `assets/svelte/consumers/dynamicRoutingDocs.ts`.

Update this .ts file to include the new sink type and its routable fields.

### [Frontend] Update the sink card

We need to update the sink card to show the routable fields.

See `assets/svelte/sinks/gcp_pubsub/GcpPubsubSinkCard.svelte` for an example.

### [Docs] Routing docs

Add the new supported sink type to `docs/reference/routing.mdx`

Update the reference for the docs to discuss routable fields, ie. `docs/reference/sinks/kafka.mdx`

### [Backend] Improve Test Connection methods

For sinks that support dynamic routing, improve the testing mechanism so we test credentials or permissions without requiring specific resource access. This is needed because dynamic routing may not have specific topics/queues configured at setup time.

Example: `test_credentials_and_permissions/1` in `lib/sequin/aws/sns.ex`

### [Backend] Update consumer form validation

Update the consumer form validation logic in `lib/sequin_web/live/components/consumer_form.ex` to handle both static and dynamic routing modes properly when testing sink connections.

For static mode: Test specific resource access (e.g., topic permissions)
For dynamic mode: Test general service permissions (e.g., list topics)
