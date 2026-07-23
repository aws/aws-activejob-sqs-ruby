# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # Raised when a job with a positive delay is enqueued to a FIFO queue.
      # SQS FIFO queues do not support per-message delays (DelaySeconds),
      # only a queue-level delay configured on the queue itself.
      class FifoDelayNotSupportedError < StandardError; end
    end
  end
end
