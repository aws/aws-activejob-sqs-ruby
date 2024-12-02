# ActiveJob with Amazon Simple Queue Service

[![Gem Version](https://badge.fury.io/rb/aws-activejob-sqs.svg)](https://badge.fury.io/rb/aws-activejob-sqs)
[![Build Status](https://github.com/aws/aws-activejob-sqs-ruby/workflows/CI/badge.svg)](https://github.com/aws/aws-activejob-sqs-ruby/actions)
[![Github forks](https://img.shields.io/github/forks/aws/aws-activejob-sqs-ruby.svg)](https://github.com/aws/aws-activejob-sqs-ruby/network)
[![Github stars](https://img.shields.io/github/stars/aws/aws-activejob-sqs-ruby.svg)](https://github.com/aws/aws-activejob-sqs-ruby/stargazers)

This gem contains [ActiveJob](https://guides.rubyonrails.org/active_job_basics.html)
adapters for Amazon Simple Queue Service (SQS).

## Installation

Add this gem to your Rails project's Gemfile:

```ruby
gem 'aws-sdk-rails', '~> 4'
gem 'aws-activejob-sqs', '~> 0'
```

Then run `bundle install`.

This gem also brings in the following AWS gems:

* `aws-sdk-sqs`

You will have to ensure that you provide credentials for the SDK to use. See the
latest [AWS SDK for Ruby Docs](https://docs.aws.amazon.com/sdk-for-ruby/v3/api/index.html#Configuration)
for details.

If you're running your Rails application on Amazon EC2, the AWS SDK will
check Amazon EC2 instance metadata for credentials to load. Learn more:
[IAM Roles for Amazon EC2](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html)

## Configuration

To use SQS as your queuing backend, simply set the `active_job.queue_adapter`
to `:sqs`.

```ruby
# config/environments/production.rb
config.active_job.queue_adapter = :sqs
```

To use the non-blocking (async) adapter, set `active_job.queue_adapter` to
`:sqs_async`. If you have a lot of jobs to queue or you need to avoid the extra
latency from an SQS call in your request then consider using the async adapter.

```ruby
# config/environments/production.rb
config.active_job.queue_adapter = :async_sqs
```

You can also set the adapter for a single job:

```ruby
class YourJob < ApplicationJob
  self.queue_adapter = :sqs
  #....
end
```

You also need to configure a mapping of ActiveJob queue names to SQS Queue URLs:

```yaml
# config/aws_sqs_active_job.yml
backpressure: 5 # configure global options for poller
max_messages: 3
queues:
  default: 
    url: 'https://my-queue-url.amazon.aws'
    max_messages: 2 # queue specific values override global values
```

For a complete list of configuration options see the
[Aws::ActiveJob::SQS::Configuration](https://docs.aws.amazon.com/sdk-for-ruby/aws-activejob-sqs/api/Aws/ActiveJob/SQS/Configuration.html)
documentation.

You can configure SQS Active Job either through the environment, yaml file or
through code in your `config/<env>.rb` or initializers.

For file based configuration, you can use either
`config/aws_sqs_active_job/<Rails.env>.yml` or `config/aws_sqs_active_job.yml`.
The yaml files support ERB.

To configure in code:

```ruby
Aws::ActiveJob::SQS.configure do |config|
  config.logger = Rails.logger
  config.max_messages = 5
  config.client = Aws::SQS::Client.new(region: 'us-east-1')
end
```

SQS Active Job loads global and queue specific values from your
environment. Global keys take the form of:
`AWS_ACTIVE_JOB_SQS_<KEY_NAME>` and queue specific keys take the
form of: `AWS_ACTIVE_JOB_SQS_<QUEUE_NAME>_<KEY_NAME>`.
<QUEUE_NAME> is case-insensitive and is always down cased. Configuring
non-snake case queues (containing upper case) through ENV is
not supported.

Example:

```shell
export AWS_ACTIVE_JOB_SQS_MAX_MESSAGES = 5
export AWS_ACTIVE_JOB_SQS_DEFAULT_URL = https://my-queue.aws
```

## Usage

To queue a job, you can just use standard ActiveJob methods:

```ruby
# To queue for immediate processing
YourJob.perform_later(args)

# or to schedule a job for a future time:
YourJob.set(wait: 1.minute).perform_later(args)
```

**Note**: Due to limitations in SQS, you cannot schedule jobs for
later than 15 minutes in the future.

### Retry Behavior and Handling Errors

See the Rails ActiveJob Guide on
[Exceptions](https://guides.rubyonrails.org/active_job_basics.html#exceptions)
for background on how ActiveJob handles exceptions and retries.

In general - you should configure retries for your jobs using
[retry_on](https://api.rubyonrails.org/classes/ActiveJob/Exceptions/ClassMethods.html#method-i-retry_on).
When configured, ActiveJob will catch the exception and reschedule the job for
re-execution after the configured delay. This will delete the original
message from the SQS queue and requeue a new message.

By default SQS ActiveJob is configured with `retry_standard_error` set to `true`
and will not delete messages for jobs that raise a `StandardError` and that do
not handle that error via `retry_on` or `discard_on`. These job messages will
remain on the queue and will be re-read and retried following the SQS Queue's
configured
[retry and DLQ settings](https://docs.aws.amazon.com/lambda/latest/operatorguide/sqs-retries.html).
If you do not have a DLQ configured, the message will continue to be attempted
until it reaches the queues retention period.  In general, it is a best practice
to configure a DLQ to store unprocessable jobs for troubleshooting and re-drive.

If you want failed jobs that do not have `retry_on` or `discard_on` configured
to be immediately discarded and not left on the queue, set `retry_standard_error`
to `false`.

When using the Async adapter, you may want to configure a
`async_queue_error_handler` to handle errors that may occur when queuing jobs. 
See
[Aws::ActiveJob::SQS::Configuration](https://docs.aws.amazon.com/sdk-for-ruby/aws-activejob-sqs/api/Aws/ActiveJob/SQS/Configuration.html)
for documentation.

### Running workers - Polling for jobs

To start processing jobs, you need to start a separate process
(in additional to your Rails app) with `bin/aws_sqs_active_job`
(an executable script provided with this gem).  You need to specify the queue to
process jobs from:

```sh
RAILS_ENV=development bundle exec aws_sqs_active_job --queue default
```

To see a complete list of arguments use `--help`.

You can kill the process at any time with `CTRL+C` - the processor will attempt
to shutdown cleanly and will wait up to `:shutdown_timeout` seconds for all
actively running jobs to finish before killing them.

**Note**: When running in production, its recommended that use a process
supervisor such as [foreman](https://github.com/ddollar/foreman), systemd,
upstart, daemontools, launchd, runit, etc.

### Serverless workers: Processing jobs using AWS Lambda

Rather than managing the worker processes yourself, you can use Lambda with an
SQS Trigger. With
[Lambda Container Image Support](https://aws.amazon.com/blogs/aws/new-for-aws-lambda-container-image-support/)
and the lambda handler provided with this gem, it's easy to use lambda to run
ActiveJobs for your dockerized rails app (see below for some tips). 

All you need to do is:
1. include the [aws_lambda_ric gem](https://github.com/aws/aws-lambda-ruby-runtime-interface-client)
2. Push your image to ECR
3. Create a lambda function from your image (see the lambda docs for details).
4. Add an SQS Trigger for the queue(s) you want to process jobs from.
5. Set the ENTRYPOINT to `/usr/local/bundle/bin/aws_lambda_ric` and the CMD
to `config/environment.Aws::ActiveJob::SQS.lambda_job_handler` - this will load
Rails and then use the lambda handler. You can do this either as function config
or in your Dockerfile.

There are a few
[limitations/requirements](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html#images-reqs)
for lambda container images: the default lambda user must be able
to read all the files and the image must be able to run on a read only file system.
You may need to disable bootsnap, set a HOME env variable and
set the logger to STDOUT (which lambda will record to cloudwatch for you).

You can use the RAILS_ENV to control environment.  If you need to execute
specific configuration in the lambda, you can create a ruby file and use it
as your entrypoint:

```ruby
# app.rb
# some custom config

require_relative 'config/environment' # load rails

# Rails.config.custom....
# Aws::ActiveJob::SQS.config....

# no need to write a handler yourself here, as long as
# aws-sdk-rails is loaded, you can still use the
# Aws::ActiveJob::SQS.config.lambda_job_handler

# To use this file, set CMD:  app.Aws::ActiveJob::SQS.config.lambda_job_handler
```

### Using FIFO queues

If the order in which your jobs executes is important, consider using a
[FIFO Queue](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html).
A FIFO queue ensures that messages are processed in the order they were sent
(First-In-First-Out) and exactly-once processing (ensuring duplicates are never
introduced into the queue). To use a fifo queue, simply set the queue url
(which will end in ".fifo") in your config.

When using FIFO queues, jobs will NOT be processed concurrently by the poller
to ensure the correct ordering. Additionally, all jobs on a FIFO queue will be queued
synchronously, even if you have configured the `sqs_async` adapter.

#### Message Deduplication ID

FIFO queues support [Message deduplication ID](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/using-messagededuplicationid-property.html),
which is the token used for deduplication of sent messages. If a message with a
particular message deduplication ID is sent successfully, any messages sent with
the same message deduplication ID are accepted successfully but aren't delivered
during the 5-minute deduplication interval.

If necessary, the deduplication key used to create the message deduplication ID
can be customized:

```ruby
Aws::ActiveJob::SQS.configure do |config|
  config.excluded_deduplication_keys = [:job_class, :arguments]
end

# Or to set deduplication keys to exclude for a single job:
class YourJob < ApplicationJob
  include Aws::ActiveJob::SQS
  deduplicate_without :job_class, :arguments
  #...
end
```

By default, the following keys are used for deduplication keys:

```
job_class, provider_job_id, queue_name, priority, arguments,
executions, exception_executions, locale, timezone, enqueued_at
```

Note that `job_id` is NOT included in deduplication keys because it is unique
for each initialization of the job, and the run-once behavior must be guaranteed
for ActiveJob retries. Even without setting job_id, it is implicitly excluded
from deduplication keys.

#### Message Group IDs

FIFO queues require a message group id to be provided for the job. It is determined by:
1. Calling `message_group_id` on the job if it is defined
2. If `message_group_id` is not defined or the result is `nil`, the default value will be used.
You can optionally specify a custom value in your config as the default that will be used by all jobs.
