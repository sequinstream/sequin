<div align="center">

<img src="https://sqn-img-svr.eric-65f.workers.dev/?page=changelog" alt="Sequin" height="80" />

</div>

# Release Notes

This document details changes on a weekly basis. For notes on Docker and CLI version releases, see the [GitHub Releases](https://github.com/sequinstream/sequin/releases) page.

# Changelog

Sequin provides fast, real-time Postgres change data capture into streams and queues.

This log is updated every Friday to track our feature releases and updates week by week. [Subscribe](https://sequinstream.com/#newsletter) to get updates in your inbox.

## June 27, 2025
[v0.10.6...v0.11.1](https://github.com/sequinstream/sequin/releases/tag/v0.11.1)

### Enhanced Routing Sink Types

Routing is a key feature of Sequin. It allows you to route messages to different destinations based on the contents of the message.

We've added mechanisms to increase the power of existing routing functions and pave the way for more routing functions in the future.

<details>

<summary>Fixes and improvements</summary>

### Improved

Improved capabilities via Routing functions:

* HTTP Push: Now support providing custom **dynamic headers** and **HTTP method**.
* Redis String: You can now customize the Redis action, and key expire time.
* NATS: Allow customizing the **full subject** and headers

</details>

## June 20, 2025
[v0.10.2...v0.10.5](https://github.com/sequinstream/sequin/releases/tag/v0.10.5)

### S2 Sink

Sequin now supports [S2](https://s2.dev/blog/sequin) as a sink destination!

<div align="center">
<img src="https://github.com/sequinstream/sequin/blob/main/docs/images/changelog/2025-06-20/s2-sink.png" alt="Sequin now natively integrates with S2" width="600" />
</div>

S2 is fully supported as a new sink in the console, API, and YAML. Read the [quickstart](https://sequinstream.com/docs/how-to/stream-postgres-to-s2#create-a-basin-and-stream) and [reference](https://sequinstream.com/docs/reference/sinks/s2) to learn how to get up and running with an S2 sink today.

#### When should you reach for S2

S2 (a.k.a. Stream Storage) is a streaming platform - like what S3 provides for blob storage, but for data in motion. Under the hood, S2 uses object storage for durability. This design provides throughput that rivals Kafka (125 MiBps / stream), at a lower cost, and with unlimited streams (no brokers and partitions). It comes with a simple API to append and read records from streams.

Consider an S2 stream when you'd otherwise reach for any other durable stream like Kafka, Kinesis, GCP Pub/Sub, or Azure Event Hubs.

<details>

<summary>Fixes and improvements</summary>

### Improved

* The pool size for source databases is now configurable.
* Improvements to Sequin's performance.
* Improved how Sequin buffers messages when a replication slot is faster than a sink can handle, resulting in much smoother performance under heavy load.
* Increase the throughput of messages through filter functions.
* Tweaked default batch sizes for sinks to optimize performance.

### Fixed

* Properly handle situations where a database or publication contain no tables.
* Handle errors when a deprecated YAML format is applied or planned.
* Fix an issue where filter and routing functions weren't included in YAML exports.
* Show a helpful error when attempting to backfill a table with no PKs.

</details>

## June 13, 2025
[v0.8.26...v0.10.1](https://github.com/sequinstream/sequin/releases/tag/v0.10.1)

### Advanced table and schema inclusion / exclusion

Building on our recent support for sinks with multiple source tables, we now give you fine-grained control over which schemas and tables flow into each sink:

<div align="center">
<img src="https://github.com/sequinstream/sequin/blob/main/docs/images/changelog/2025-06-13/advanced-source-controle.png" alt="Advanced tools for adding multiple tables to a sink" width="600" />
</div>

* **All tables in the publication:** Capture changes from every table exposed by the publication. When new schemas or tables are added, they are automatically included.
* **Include *only* selected schemas or tables:** Choose the schemas and tables you care about; changes from only those schemas and tables will be processed in your sink.
* **Exclude specific schemas or tables:** Start with everything in the publication, then list the schemas or tables you want to omit. Future schemas and tables will be included in your sink unless they appear on your exclusion list.

These settings make it easier to create and configure one sink to power your entire CDC use case. These new configuration options are supported in the Sequin console, `sequin.yml`, and in the API.

### Trace for all sinks

Trace is now available on all sinks! Expanding on the observability in the messages tab, trace surfaces the underlying logging for a given sink to give you more insight into the sinks state.

As we make each sink more powerful with support for many tables and custom code in functions, we're also adding more tooling to debug and observe how is running.

<details>

<summary>Fixes and improvements</summary>

### Improved

* `sequin config plan` now shows the diff for sensitive values, like passwords
* Use key up and down arrow keyboard commands to move through messages and trace logs in the console.
* Added exponential back-off to HTTP Webhook sinks.
* Redis Stream sinks now support transform functions.
* Make it easier to retrieve and remove test messages when editing functions.
* Add support for `infinity_timestamps` in Postgres.
* Optimize Sequin performance on smaller boxes.

### Fixed

* Ensure the change retention page doesn't break if a table is removed from the database.

</details>

## June 6, 2025
[v0.8.13...v0.8.25](https://github.com/sequinstream/sequin/releases/tag/v0.8.25)

### Multiple tables per sink

Sequin now supports [multiple source tables](https://sequinstream.com/docs/reference/sinks/overview#source-tables) in one sink. You can now specify an entire schema to include in your sink, and Sequin will sink every table in the schema that is also in the publication.

<div align="center">
<img src="https://github.com/sequinstream/sequin/blob/main/docs/images/changelog/2025-06-06/multiple-tables.png" alt="Add multiple tables to one sink" width="600" />
</div>

Sinking a schema is supported in our [API](https://sequinstream.com/docs/management-api/sink-consumers/create#param-table) and in [sequin.yml](https://sequinstream.com/docs/reference/sequin-yaml#sink-source). You'll also see that we've moved backfills into a new tab to support multi-table backfills with improved monitoring:

<div align="center">
<img src="https://github.com/sequinstream/sequin/blob/main/docs/images/changelog/2025-06-06/backfill-tab.png" alt="New backfill tab to manage and observe multiple table backfills" width="600" />
</div>

### Trace for TypeSense

The Trace tab is now available for TypeSense sinks. Trace shows the recent logs for a sync for improved observability and debugging.

<details>

<summary>Fixes and improvements</summary>

### Improved

* Allow column filters to be edited on active sinks
* Allow `Kafka.test_connection` to auto-create topics if the cluster allows it.
* Add error handling when attempting to group by a column that no longer exists.
* Improve how sensitive keys are removed from logger metadata
* Major performance improvements

### Fixed

* Fixed how we handle TOAST values that are nil
* Fixed error messaging if Sequin doesn't have `usage` permissions on a schema

</details>

## May 30, 2025
[v0.8.8...v0.8.12](https://github.com/sequinstream/sequin/releases/tag/v0.8.12)

### Added

* Added support for [AWS Kinesis](https://sequinstream.com/docs/reference/sinks/kinesis) sinks.
* The **Trace Tab** allows you to observe all the logs for a given sink. It's rolling out for HTTP Webhook sinks now - with support for other sinks coming soon!
* The [management API](https://sequinstream.com/docs/management-api/sink-consumers/get) now shows the `health` of sinks.
* You can now edit test message payloads to quickly test your functions.

### Changed

* Multiple improvements to the Sequin console including new components for adding and showing functions, ability to easily open sinks in new tabs, and persistent pagination.
* Major performance improvements

### Fixed

* Fixed an error in loading the `CONFIG_FILE_PATH`
* Fixed an issue where `PATCH` would drop functions from a sink.
* Fixed a bug that showed some messages as available when they are not.
* Fix edit flow for path transforms

## May 23, 2025
[v0.7.32...v0.8.7](https://github.com/sequinstream/sequin/releases/tag/v0.8.7)

### Added

* [Filter functions](https://sequinstream.com/docs/reference/filters#filter-functions) provide advanced logic (using mini Elixir) for filtering which changes in your database are delivered to your sink.
* First release of [routing functions](https://sequinstream.com/docs/reference/routing) for HTTP Webhook and Redis String sinks. Routing functions dynamically direct messages to different destinations based on the contents of the message.

### Changed

* Show publication and replication slot name for databases in the console.
* For NATs sinks, use `idempotency_key` as `Nats-Msg-Id` for deduplication.
* Transforms are now a sub-type of functions to make room for filter and routing functions.
* Improved how we display secret errors when loading `sequin.yml`

### Fixed

* Fixed how we handle database.ssl=false.
* Fixed invited on self-hosted when self-signup is disabled.

## May 16, 2025
[v0.7.16...v0.7.31](https://github.com/sequinstream/sequin/releases/tag/v0.7.31)

### Added

* Sequin can now connect to [Postgres Replicas](https://sequinstream.com/docs/reference/databases#using-sequin-with-a-replica).
* Transform functions can now be removed from a sink.
* Added a CLI page to the console

### Changed

* Improve TypeSense sink endpoint validation and error handling
* Improved error formatting and messages when running `sequin config export/plan/apply` in the Sequin CLI.
* Improved error handling and messages when writing functions with mini elixir
* Improved how sensitive fields are filtered out of logs.
* Improved how transform functions, `sinks.max_retry_count`, `sinks.active_backfill` and `http_endpoints.encrypted_headers` are processed in `sequin.yml`
* Sink filters now show the correct comparison values when working with `IS NULL` or `IS NOT NULL` expressions.

### Fixed

* Fixed only display message not_visible_until date if its defined.
* Fixed pagination on list sink page and in the messages tab
* Fixed `null` value handling when processing the WAL.

## May 09, 2025
[v0.7.3...v0.7.14](https://github.com/sequinstream/sequin/releases/tag/v0.7.14)

### Added

* Added support for [Redis String](https://sequinstream.com/docs/reference/sinks/redis-string) sinks
* HTTP Webhook sinks improvements including the ability `batch` messages and configure the `max_retry_count` to limit Webhook delivery attempts.
* You can now optimize Sequin performance by changing the memory limit for sinks.
* Added the [`SEQUIN_LOG_FORMAT`](https://sequinstream.com/docs/reference/configuration#general-configuration) setting to the config to easily connect Sequin to tools like DataDog.
* Added idempotency_key to message payloads to improve exactly-once processing mechanics.

### Changed

* Set a default port to REDIS_URL if it's missing.
* Improved how Sequin handles database reconnects to improve performance.
* Added basic authentication to Prometheus
* Improvements to error messaging when creating replication slots, connecting databases, and checking database health.
* Function now accept `_` variables

### Fixed

* Fix 'Copy for ChatGPT' button when creating functions.
* Fix reporting of replication slot size.

## May 02, 2025
[v0.6.110...v0.7.2](https://github.com/sequinstream/sequin/releases/tag/v0.7.2)

### Added

* Added CRUD support for databases in the [management API](https://sequinstream.com/docs/management-api/postgres_databases/list).
* Added support Postgres 13+.
* Added backfill lag metrics to help with monitoring backfills.

### Changed

* Changed timestamp precision to microseconds for Postgres `timestamp` and `timestamptz`.
* Improved CLI diffs display and error handling on `config plan/apply`.

### Fixed

* Ensure transform functions startup properly.
* Fixed database string parsing in the console.

## April 25, 2025
[v0.6.103...v0.6.109](https://github.com/sequinstream/sequin/releases/tag/v0.6.109)

### Added

* Added support for three new sink destinations: [AWS SNS](https://sequinstream.com/docs/reference/sinks/sns), [Typesense](https://sequinstream.com/docs/reference/sinks/typesense), and [Elasticsearch](https://sequinstream.com/docs/reference/sinks/elasticsearch).

### Changed

* Improved memory utilization, especially when hitting memory limits that require a disconnect from the replication slot.
* Added support for modules like `Base` to functions.
* Renamed current Redis sink to Redis Stream to make way for the new Redis String sink.

### Fixed

* Fixed Postgres array parsing to handle complex data types more accurately.
* Improved in-console experience around editing function transforms.
* Fixed some YAML parsing errors in `sequin.yaml` configuration files around anchors and interpolation.
* Resolved some TLS configuration issues in CLI when working with `localhost`.

## April 18, 2025
[v0.6.96...v0.6.102](https://github.com/sequinstream/sequin/releases/tag/v0.6.102)

We're introducing [routing functions](https://sequinstream.com/docs/reference/routing) which allow you to write custom logic on the message content to direct the message to different destinations. For instance, you can send `inserts` to a `POST` endpoint and `updates` to a `PUT`. We're starting with HTTP Webhook and Redis String sinks, with more coming soon!

### Added

* Added routing function support for HTTP Webhook and Redis String sinks.
* Added the [`sequin config interpolate`](https://sequinstream.com/docs/reference/cli/overview#commands) command to the CLI to output a YAML file with all environment variables interpolated.

### Changed

* Database connection failures during startup now provide detailed error messages to help diagnose connectivity issues.
* Improved replica identity validation for partitioned tables to ensure proper change data capture.

### Fixed

* Resolved issue in change retention where WAL events for the same record were incorrectly coalesced.
* Eliminated race condition during startup where transform functions were loaded simultaneously.

## April 11, 2025
[v0.6.89...v0.6.95](https://github.com/sequinstream/sequin/releases/tag/v0.6.95)

### Added

* Self-hosted deployments can now be configured with [GitHub logins](https://sequinstream.com/docs/reference/configuration#oauth-configuration).
* The [management API](https://sequinstream.com/docs/management-api/introduction) now supports CRUD on HTTP endpoints, sinks, and backfills.
* `sequin.yml` supports [environment variable substitutions](https://sequinstream.com/docs/reference/sequin-yaml#environment-variable-substitution). This makes it easier to keep secrets out of your `sequin.yml`. And makes it easier to re-use your `sequin.yml` in multiple environments.
* You can now [provision an API Token](https://sequinstream.com/docs/reference/sequin-yaml#api-token-configuration) in the `sequin.yml`. This can be useful for dev and CI workflows. You can provision a Sequin account with an API token then make subsequent API calls to your Sequin instance.

### Changed

* Improved logging configuration: The log level can now be set via the LOG_LEVEL environment variable across all environments, with sensible defaults

### Fixed

* Fix config file interpolation
* Various CLI improvements around config plan/apply

## Week of March 31, 2025
[v0.6.69...v0.6.84](https://github.com/sequinstream/sequin/releases/tag/v0.6.84)

We're beginning to roll out **[transform functions](https://sequinstream.com/docs/reference/transforms)**. Transform functions let you modify messages before they are delivered to a sink.

We also rolled out a [metrics endpoint](https://sequinstream.com/docs/reference/metrics) for Prometheus metrics in self-hosted deployments.

### Added

* Add basic transform function support.
* Add [/metrics](https://sequinstream.com/docs/reference/metrics) endpoint for Prometheus metrics.
* [Transaction annotations](https://sequinstream.com/docs/reference/annotations) are a powerful feature that let you add arbitrary metadata to a transaction.
* Sequin now listens for relation messages from Postgres. When we see a relation message, we re-fetch all tables and columns, meaning message shapes are updated right away.

### Fixed

* Sequin will now await external databases to be ready when booted with a `sequin.yml` file. See more about this feature [in the docs](https://sequinstream.com/docs/reference/sequin-yaml#database-connection-retry).

## Week of March 10, 2025

### Added

* Support YAML anchors in `sequin.yml`.

### Fixed

* Add support for GCP standalone Redis.
* Support databases where the `wal_sender_timeout` is set to `0` (unlimited). We now advance the replication slot in these cases.
* Flush dormant messages to disk. Now, if messages are stuck in our buffer (e.g. they are blocked waiting for downstream messages to deliver), we'll flush them to disk. This allows us to advance the replication slot.
* Fixed some issues with redelivery buttons on the "Messages" tab.

## Week of March 3, 2025

After all the performance improvements of the past few weeks, Sequin's now able to sustain 40k messages per second, or 40MB/s of throughput.

### Added

* Use heartbeat messages to detect (and recover) stale replication slot connections.

### Changed

* A lot more performance improvements to our replication pipeline.
* Improve backfill performance by adding pre-fetching of next pages.
* Improve health alerts around backfill errors.
* Add `SERVER_CHECK_ORIGIN` flag (default `false`). This makes it easier to get Sequin up and running for the first time.

### Fixed

* Improved calculation of throughput metrics. Now, we use the outgoing JSON-encoded payload size.

## Week of February 17, 2025

### Added

* The backfill process now dynamically tunes the page size of its queries to maximize throughput.
* Add batch support for GCP Pub/Sub, grouped by `ordering_key`.

### Changed

* Improved backfill back-pressure in situations where the replication slot is very busy.

## Week of February 10, 2025

### Added

* Support for partition tables, even when `publish_via_partition_root` is not set.
* Support for `pg_vector` columns.
* Adding pooling to our Redis connection for improved performance.
* Added a graph for bandwidth on sink details pages.
* Add support for hovering over throughput graphs to see the exact values.

### Changed

* Added support for paused sinks. Unlike disabled sinks, paused sinks continue to accumulate messages, but do not deliver them. It is intended as a temporary measure.
* Improve back-pressure and performance when dealing with a very large transaction.

### Fixed

* Various clean-ups/tweaks after our major performance improvement changes last week.
* Fix backfill statistics for very large tables.

## Week of February 3, 2025

We rolled out a major improvement in performance that we've been working towards over the past few weeks. With this release, our data pipeline operates entirely in-memory, with some bookkeeping in Redis. It does so while still maintaining an at-least-once (approaching exactly-once) delivery guarantee.

### Added

* Added replication lag health checks. Now, if we detect that Sequin is lagging behind your database, you'll be alerted in the console.
* Added a replication lag gauge to the database details page.

### Changed

* Major performance improvements (above).

### Fixed

* Fixed an issue with the "Redeliver all messages" button on the "Messages" tab.
* Fixed an issue where we could have multiple heartbeat timers and messages in flight at once.

## Week of January 27, 2025

We've been focused on improving performance. Most users should note a 30%+ improvement in messages per second throughput after this week's changes.

### Added

* Azure EventHubs sink.
* Added delivery verification for sinks to prove the pipeline's at-least-once delivery guarantee.

### Changed

* Improved pipeline performance by reducing round-trips to Redis.
* Various other performance improvements.

## Week of January 20, 2025

This week, we rolled out even more improvements to our health checks and alerting. We're also very focused on improving performance, and made significant improvements to the speed of our pipeline.

### Changed

* Various improvements and additions to health checks and alerting.
* Improve performance of the data pipeline, so we can handle more messages per second.
* Improve how we measure and report latency metrics.

## Week of January 13, 2025

This week, we greatly improved the "Messages" tab in the console. The "Messages" tab is where you can see messages that are about to be delivered, have failed to deliver, or have been delivered to a sink.

### Added

* Add ability to redeliver messages right away from the console.
* Notice if your Sequin deployment is behind the latest version.
* In GCP Pub/Sub, add support for using the GCP Pub/Sub local emulator.

### Changed

* General improvements to the messages tab, as well as the information and actions on individual messages.

## Week of January 6, 2025

This week was all about improving our health checks and surfacing more information in the console. We also added more controls around pausing and resuming different parts of the pipeline.

### Added

* Support for either pausing or disabling sinks.
* Support for pausing Postgres replication slots.
* Add a refresh button to all health status components.
* Add new on-going health checks for sink and database configuration.
* Add ability to set a contact email for an account.

### Changed

* Various speed and performance improvements.

## Week of December 30, 2024

### Added

* Support auth credentials for NATS.
* Add heartbeat messages to replication slots for improved health checks.
* Improved alerting of the Sequin team in Sequin Cloud.

### Changed

* Improved the logs we emit across the system by adding metadata. Our logs follow the "[wide structured logging](https://stripe.com/blog/canonical-log-lines)" style.

### Fixed

* Heartbeat messages resolve issues that can arise when a replication slot is dormant.

## Week of December 23, 2024

### Added

* Support for using Sequin with a clustered Redis.
* Improved support for running Sequin in GCP (GCP Redis support).
* Add ability for users to change their password.

### Changed

* Refactored backfill process to use a strongly consistent watermark strategy. Backfill messages are now zipped in strict (safe) order with replication messages.

### Fixed

* Improvements to the sink throughput graph.

## Week of December 16, 2024

### Added

* Added support for [RabbitMQ](https://sequinstream.com/docs/quickstart/rabbitmq) as a sink.
* Overhauled parsing and error messages around [Sequin configuration](https://sequinstream.com/docs/reference/configuration).
* Added a console graph for sink throughput.

### Changed

* Improvements to overall pipeline speed and performance, especially under high load.
* Improvements to backfill speed and performance.
* Improved the default subject pattern used for the NATS sink.

### Fixed

* Resolved issues with IPv6 support in the NATS sink.
* Fixed an issue with certain sort columns in the backfill process.

## Week of December 9, 2024

### Added

* **Support for active-passive deployments**: You can now run multiple Sequin instances simultaneously for high availability. One Sequin instance will actively run replication, while the others will be on standby. During a failure of the primary, Sequin will automatically failover to a standby, ensuring continuous CDC operations. Read more in the [docs](https://sequinstream.com/docs/reference/configuration#active-passive-architecture).
* **Expanded Kafka support**: We tuned Sequin to work with Redpanda and added a [quick start](https://sequinstream.com/docs/quickstart/redpanda).

### Changed

* Improved `sequin config export` command.
* Better UX for backfills with improved progress metrics, making it easier for users to track completion status and estimate remaining time.
* In "Messages" tab, improved clarity of labels for message states.

### Fixed

* Resolved issues with running connection tests in the sink form.
* Add support for TLS connections to Sequin's config Redis.
