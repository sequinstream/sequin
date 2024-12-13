# Changelog

Sequin provides fast, real-time Postgres change data capture into streams and queues.

This log is organized by week to track our feature releases and updates.

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