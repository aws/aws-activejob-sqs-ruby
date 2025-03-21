Unreleased Changes
------------------

* Issue - Remove `aws_sqs_active_job` executable.
* Issue - Handle `Time` objects correctly when using `ActiveJob.perform_all_later`.

1.0.1 (2024-12-23)
------------------

* Issue - Add deprecated `aws_sqs_active_job` executable to aid in migration.
* Issue - Support legacy `queue: 'url'` config in file to aid in migration.

1.0.0 (2024-12-13)
------------------

* Feature - Support polling on multiple queues. (#4)
* Feature - Support running without Rails. (#5)
* Feature - Replace `retry_standard_errors` with `poller_error_handler`. (#6)
* Feature - Support per queue configuration. (#4)
* Feature - Support loading global and queue specific configuration from ENV. (#3)

0.1.1 (2024-12-02)
------------------

* Feature - Add lifecycle hooks for Executor.

0.1.0 (2024-11-16)
------------------

* Feature - Initial version of this gem.
