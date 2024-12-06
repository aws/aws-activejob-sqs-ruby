# frozen_string_literal: true

require 'optparse'

module Aws
  module ActiveJob
    module SQS
      # options for the aws_active_job_sqs CLI.
      # @api private
      CliOptions = Struct.new(
        :boot_rails,
        :threads,
        :backpressure,
        :max_messages,
        :visibility_timeout,
        :shutdown_timeout,
        :require,
        :queues,
        keyword_init: true
      ) do
        def self.parse(argv)
          out = new(boot_rails: true, queues: [])
          parser = ::OptionParser.new do |opts|
            queues_option(opts, out)
            threads_option(opts, out)
            backpressure_option(opts, out)
            max_messages_option(opts, out)
            visibility_timeout_option(opts, out)
            shutdown_timeout_option(opts, out)
            boot_rails_option(opts, out)
            require_option(opts, out)
          end

          parser.banner = 'aws_active_job_sqs [options]'
          parser.on_tail '-h', '--help', 'Show help' do
            puts parser
            exit 1
          end

          parser.parse(argv)
          out
        end

        def self.require_option(opts, out)
          opts.on('-r', '--require STRING', String,
                  'Additional file to require before starting the poller.  Can be used to define/load job classes whith --no-rails.') do |a|
            out[:require] = a
          end
        end

        def self.boot_rails_option(opts, out)
          opts.on('--[no-]rails [FLAG]', TrueClass,
                  'When set boots rails before running the poller.') do |a|
            out[:boot_rails] = a.nil? ? true : a
          end
        end

        def self.shutdown_timeout_option(opts, out)
          opts.on('-s', '--shutdown_timeout INTEGER', Integer,
                  'The amount of time to wait for a clean shutdown.  Jobs that are unable to complete in this time will not be deleted from the SQS queue and will be retryable after the visibility timeout.') do |a|
            out[:shutdown_timeout] = a
          end
        end

        def self.visibility_timeout_option(opts, out)
          opts.on('-v', '--visibility_timeout INTEGER', Integer,
                  'The visibility timeout is the number of seconds that a message will not be processable by any other consumers. You should set this value to be longer than your expected job runtime to prevent other processes from picking up an running job.  See the SQS Visibility Timeout Documentation at https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html.') do |a|
            out[:visibility_timeout] = a
          end
        end

        def self.max_messages_option(opts, out)
          opts.on('-m', '--max_messages INTEGER', Integer,
                  'Max number of messages to receive in a batch from SQS.') do |a|
            out[:max_messages] = a
          end
        end

        def self.backpressure_option(opts, out)
          opts.on('-b', '--backpressure INTEGER', Integer,
                  'The maximum number of messages to have waiting in the Executor queue. This should be a low, but non zero number.  Messages in the Executor queue cannot be picked up by other processes and will slow down shutdown.') do |a|
            out[:backpressure] = a
          end
        end

        def self.threads_option(opts, out)
          opts.on('-t', '--threads INTEGER', Integer,
                  'The maximum number of worker threads to create.  Defaults to 2x the number of processors available on this system.') do |a|
            out[:threads] = a
          end
        end

        def self.queues_option(opts, out)
          opts.on('-q', '--queue STRING', 'Queue(s) to poll. You may specify this argument multiple times to poll multiple queues.  If not specified, will start pollers for all queues defined.') do |a|
            out[:queues] << a.to_sym
          end
        end
      end
    end
  end
end
