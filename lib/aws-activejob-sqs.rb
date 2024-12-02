# frozen_string_literal: true

require_relative 'active_job/queue_adapters/sqs_adapter'
require_relative 'active_job/queue_adapters/sqs_adapter/params'
require_relative 'active_job/queue_adapters/sqs_async_adapter'
require_relative 'aws/active_job/sqs/configuration'
require_relative 'aws/active_job/sqs/deduplication'
require_relative 'aws/active_job/sqs/executor'
require_relative 'aws/active_job/sqs/job_runner'
require_relative 'aws/active_job/sqs/lambda_handler'

module Aws
  module ActiveJob
    module SQS
      VERSION = File.read(File.expand_path('../VERSION', __dir__)).strip

      # @return [Configuration] the (singleton) Configuration
      def self.config
        @config ||= Configuration.new
      end

      # @yield Configuration
      def self.configure
        yield(config)
      end

      # @param queue_url [String]
      # @return [Boolean] true if the queue_url is a FIFO queue
      def self.fifo?(queue_url)
        queue_url.end_with?('.fifo')
      end

      def self.on_worker_stop(...)
        Executor.on_stop(...)
      end
    end
  end
end
