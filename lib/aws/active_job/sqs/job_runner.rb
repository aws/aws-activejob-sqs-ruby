# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # @api private
      class JobRunner
        attr_reader :id, :class_name

        def initialize(message)
          @message = message
          @job_data = ActiveSupport::JSON.load(message.data.body)
          @class_name = @job_data['job_class'].constantize
          @id = @job_data['job_id']
        end

        def run
          job = ::ActiveJob::Base.deserialize(@job_data)
          job.sqs_message = @message if job.respond_to?(:sqs_message=)
          job.perform_now
        end

        def exception_executions?
          @job_data['exception_executions'] &&
            !@job_data['exception_executions'].empty?
        end
      end
    end
  end
end
