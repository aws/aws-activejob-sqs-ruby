# frozen_string_literal: true

require 'optparse'

module Aws
  module ActiveJob
    module SQS
      # options for the aws_active_job_sqs CLI.
      # @api private
      module CliOptions
        def self.option_parser(out)
          ::OptionParser.new do |opts|
            queues_option(opts, out)
            threads_option(opts, out)
            backpressure_option(opts, out)
            max_messages_option(opts, out)
            visibility_timeout_option(opts, out)
            shutdown_timeout_option(opts, out)
            boot_rails_option(opts, out)
            require_option(opts, out)
          end
        end
        private_class_method :option_parser

        def self.parse(argv)
          out = { boot_rails: true, queues: [] }
          parser = option_parser(out)

          parser.banner = 'aws_active_job_sqs [options]'
          parser.on_tail '-h', '--help', 'Show help' do
            puts parser
            exit 1
          end

          parser.parse(argv)
          out
        end

        def self.require_option(opts, out)
          doc = 'Additional file to require before starting the poller.  ' \
                'Can be used to define/load job classes with --no-rails.'
          opts.on('-r', '--require STRING', String, doc) do |a|
            out[:require] = a
          end
        end

        def self.boot_rails_option(opts, out)
          doc = 'When set boots rails before running the poller.'
          opts.on('--[no-]rails [FLAG]', TrueClass, doc) do |a|
            out[:boot_rails] = a.nil? || a
          end
        end

        def self.shutdown_timeout_option(opts, out)
          doc = 'The amount of time to wait for a clean shutdown.  Jobs that ' \
                'are unable to complete in this time will not be deleted from ' \
                'the SQS queue and will be retryable after the visibility timeout.'
          opts.on('-s', '--shutdown_timeout INTEGER', Integer, doc) do |a|
            out[:shutdown_timeout] = a
          end
        end

        def self.visibility_timeout_option(opts, out)
          doc = 'The visibility timeout is the number of seconds that a ' \
                'message will not be processable by any other consumers. ' \
                'You should set this value to be longer than your expected ' \
                'job runtime to prevent other processes from picking up an ' \
                'running job.  See the SQS Visibility Timeout Documentation ' \
                'at https://docs.aws.amazon.com/AWSSimpleQueueService/latest/' \
                'SQSDeveloperGuide/sqs-visibility-timeout.html.'
          opts.on('-v', '--visibility_timeout INTEGER', Integer, doc) do |a|
            out[:visibility_timeout] = a
          end
        end

        def self.max_messages_option(opts, out)
          doc = 'Max number of messages to receive in a batch from SQS.'
          opts.on('-m', '--max_messages INTEGER', Integer, doc) do |a|
            out[:max_messages] = a
          end
        end

        def self.backpressure_option(opts, out)
          doc = 'The maximum number of messages to have waiting in ' \
                'the Executor queue. This should be a low, but non zero number.  ' \
                'Messages in the Executor queue cannot be picked up by other ' \
                'processes and will slow down shutdown.'
          opts.on('-b', '--backpressure INTEGER', Integer, doc) do |a|
            out[:backpressure] = a
          end
        end

        def self.threads_option(opts, out)
          doc = 'The maximum number of worker threads to create.  ' \
                'Defaults to 2x the number of processors available on this system.'
          opts.on('-t', '--threads INTEGER', Integer, doc) do |a|
            out[:threads] = a
          end
        end

        def self.queues_option(opts, out)
          doc = 'Queue(s) to poll. You may specify this argument multiple ' \
                'times to poll multiple queues.  If not specified, will ' \
                'start pollers for all queues defined.'
          opts.on('-q', '--queue STRING', doc) do |a|
            out[:queues] << a.to_sym
          end
        end
      end
    end
  end
end
