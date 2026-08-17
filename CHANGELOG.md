Unreleased Changes
------------------

1.2.0 (2026-08-17)
------------------

* Feature - Report a dropped job to the Rails error reporter and add an optional `permanent_failure_handler` so apps can keep unresolvable jobs on the queue for redelivery instead of losing them (#39).

* Issue - `perform_all_later` no longer partially enqueues a batch before raising when one of the jobs has a delay on a FIFO queue (#40).

1.1.1 (2026-08-12)
------------------

* Issue - Fixed an empty or blank `job_class_allowlist` causing every job to be rejected and deleted. A blank allowlist is now treated as no allowlist, so all jobs run.

1.1.0 (2026-08-11)
------------------

* Issue - Raise a clear error at enqueue time when a delayed job targets a FIFO queue, which does not support per-message delays (#36).
* Issue - Only classes inheriting from `ActiveJob::Base` are executed by the poller, preventing arbitrary classes named in an SQS message from being instantiated and run. Job class names are validated as constant paths and resolved without searching the namespace's ancestors, so a message cannot name a constant outside the intended namespace.
* Feature - Add optional `job_class_allowlist` configuration to restrict which job classes can be dispatched. Configurable in code, the config YAML file, or via the `AWS_ACTIVE_JOB_SQS_JOB_CLASS_ALLOWLIST` environment variable. When set, the allowlist is checked before the class name is resolved, so an excluded class is never loaded.

1.0.2 (2025-04-01)
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
