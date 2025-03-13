# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # @api private
      class JobRunner
        attr_reader :id, :class_name

        def self.queue_event_handlers
          @@queue_handlers ||= {}.tap do |handlers|
            Aws::ActiveJob::SQS.config.queues.values.each do |queue_config|
              next unless queue_config[:job_class].present?

              handlers[queue_config[:url]] = queue_config[:job_class]
            end
          end
        end

        def initialize(message)
          @job_data   = prepare_job_data(message)
          @class_name = @job_data['job_class'].constantize
          @id         = @job_data['job_id']
        end

        def run
          ::ActiveJob::Base.execute(@job_data)
        end

        def exception_executions?
          @job_data['exception_executions'] &&
            !@job_data['exception_executions'].empty?
        end

        private

        def prepare_job_data(message)
          return ActiveSupport::JSON.load(message.data.body) if is_active_job_message?(message)

          format_event_data(message)
        end

        def format_event_data(message)
          {
            'job_class' => job_class_from_config(message.queue_url),
            'job_id' => message.message_id,
            'arguments' => [message.data.to_json]
          }
        end

        # Active job messages will have message_attributes key 'aws_sqs_active_job_class'
        def is_active_job_message?(message)
          !message
            .message_attributes['aws_sqs_active_job_class']
            .nil?
        end

        def job_class_from_config(queue_url)
          return queue_event_handlers[queue_url] if queue_event_handlers[queue_url]

          raise ArgumentError, "No handler configured for queue #{queue_url}"
        end

        def queue_event_handlers
          self.class.queue_event_handlers
        end
      end
    end
  end
end
