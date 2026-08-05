# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # Raised when a job with a positive delay is enqueued to a FIFO queue.
      class FifoDelayNotSupportedError < StandardError; end
    end
  end
end
