# Changelog

Sequin provides fast, real-time Postgres change data capture into streams and queues.

This log is organized by week to track our feature releases and updates. [Subscribe](https://sequinstream.com/#newsletter) to get updates in your inbox.

## Week of March 31, 2025 

We're beginning to roll out **processors**. Processors let you modify messages before they are delivered to a sink.

We also rolled out a [metrics endpoint](https://sequinstream.com/docs/reference/metrics) for Prometheus metrics in self-hosted deployments.

### Added

* Add basic processor support.
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
