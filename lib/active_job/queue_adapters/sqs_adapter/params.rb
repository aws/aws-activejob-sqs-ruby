# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    class SqsAdapter
      # Build request parameter of Aws::SQS::Client
      # @api private
      class Params
        class << self
          def assured_delay_seconds(timestamp)
            delay = (timestamp.to_f - Time.now.to_f).floor
            raise ArgumentError, 'Unable to queue a job with a delay great than 15 minutes' if delay > 15.minutes

            [delay, 0].max
          end
        end

        def initialize(job, body)
          @job = job
          @body = body || job.serialize
        end

        def queue_url
          @queue_url ||= Aws::ActiveJob::SQS.config.url_for(@job.queue_name)
        end

        def entry
          if Aws::ActiveJob::SQS.fifo?(queue_url)
            default_entry.merge(options_for_fifo)
          else
            default_entry
          end
        end

        private

        def default_entry
          {
            message_body: ActiveSupport::JSON.dump(@body),
            message_attributes: message_attributes
          }
        end

        def message_attributes
          {
            'aws_sqs_active_job_class' => {
              string_value: @job.class.to_s,
              data_type: 'String'
            },
            'aws_sqs_active_job_version' => {
              string_value: Aws::ActiveJob::SQS::VERSION,
              data_type: 'String'
            }
          }
        end

        def options_for_fifo
          options = {}
          options[:message_deduplication_id] =
            Digest::SHA256.hexdigest(ActiveSupport::JSON.dump(deduplication_body))

          message_group_id = @job.message_group_id if @job.respond_to?(:message_group_id)
          message_group_id ||= Aws::ActiveJob::SQS.config.message_group_id_for(@job.queue_name)

          options[:message_group_id] = message_group_id
          options
        end

        def deduplication_body
          ex_dedup_keys = @job.excluded_deduplication_keys if @job.respond_to?(:excluded_deduplication_keys)
          ex_dedup_keys ||= Aws::ActiveJob::SQS.config.excluded_deduplication_keys_for(@job.queue_name)

          @body.except(*ex_dedup_keys)
        end
      end
    end
  end
end
