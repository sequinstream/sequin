# Changelog

Sequin provides fast, real-time Postgres change data capture into streams and queues.

This log is updated every Friday to track our feature releases and updates week by week. [Subscribe](https://sequinstream.com/#newsletter) to get updates in your inbox.

## May 16, 2025

### Added

* Sequin can now connect to Postgres Replicas
* Add deep linking from the a message delivery error to the messages tab.

### Changed

* Improve TypeSense sink endpoint url validation and error handling


* Changed show flash if show-ack-id message not found.




* Added use `@derive` to filter sensitive sink fields out from logs.
* Added handle un-fifo-ifying a sink's queue.
* Added clock drift metrics.
* Added busy_percent to ProcessMetrics.
* Changed only consider physical replicas as replicas.

* Changed check replication lag properly for replicas.
* Changed handle empty cond & other errors.
* Changed show 'Edit database' in title when editing.
* Changed don't log every single keepalive we receive.
* Changed oban queues.
* Changed make kickoff and system jobs unique.
* Changed format errors for sequin config export/plan/apply.
* Allow transform functions to be removed from a sink

### Fixed

* Changed in filters, do not show comparison value when user selects IS NULL or IS NOT NULL.
* Fix transforms YAML flat format.
* Fixed put back execution redis command in fun.
* Fixed improve full page forms to avoid using a modal.
* Fixed improve yaml load for `sinks.max_retry_count`, `sinks.active_backfill` and `http_endpoints.encrypted_headers`.
* Fixed message Grouping card form improvements.
* Fix pagination on sink/messages.
* Fix flaky test by increasing timeout.
* Fix consumer message serialization of struct types.
* Fixed show missing Transform Function form validation errors.
* Fix database replica primary form population.
* Fixed only display message not_visible_until date if its defined.
* Fixed handle nil confirmed_flush / restart lsns.

## May 09, 2025

### Added

* Added a Redis String sink destination
* Multiple improvements for HTTP Webhook sings including the ability `batch` messages, and configure the `max_retry_count` to limit Webhook delivery attempts.
* You can optimize Sequin performance by changing the memory limit for sinks.
* Added [`SEQUIN_LOG_FORMAT`](https://sequinstream.com/docs/reference/configuration#general-configuration) to the config so you can integrate with tools like DataDog.
* Added idempotency_key to message payloads to simplify exactly-once processing.

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

### Added

* Added CRUD support for databases in the [management API](https://sequinstream.com/docs/management-api/postgres_databases/list).
* Added support Postgres 13+.
* Added backfill lag metrics to help with monitoring backfills.

### Changed

* Changed timestamp precision to microseconds for Postgres `timestamp` and `timestamptz`.
* Improved CLI diffs display and error handling on `config plan/apply`.

### Fixed

* Ensure transform and routing functions startup properly.
* Fixed database string parsing in the console.

## April 25, 2025

### Added

* Added support for three new sink destinations: [AWS SNS](https://sequinstream.com/docs/reference/sinks/sns), [Typesense](https://sequinstream.com/docs/reference/sinks/typesense), and [Elasticsearch](https://sequinstream.com/docs/reference/sinks/elasticsearch) integration.

### Changed

* Improvements to memory utilization.
* Improved how the system behaves when it hits memory limits and needs to disconnect from the replication slot.
* Added support for modules like `Base` to functions.
* Renamed current Redis sink to Redis Stream to make way for the new Redis String sink.

### Fixed

* Fixed Postgres array parsing to handle complex data types more accurately.
* Improved in-console experience around editing function transforms.
* Fixed some YAML parsing errors in `sequin.yaml` configuration files around anchors and interpolation.
* Resolved some TLS configuration issues in CLI when working with `localhost`.

## April 18, 2025

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
