# frozen_string_literal: true

require 'aws-sdk-sqs'

module ActiveJob
  module QueueAdapters
    # Synchronous adapter for Amazon SQS ActiveJob. This adapter queues jobs synchronously (ie blocking).
    #
    # To use this adapter, set up as:
    #
    #     config.active_job.queue_adapter = :sqs
    class SqsAdapter
      def enqueue_after_transaction_commit?
        # can be removed after Rails 8
        true
      end

      def enqueue(job)
        _enqueue(job)
      end

      def enqueue_at(job, timestamp)
        delay = Params.assured_delay_seconds(timestamp)
        _enqueue(job, nil, delay_seconds: delay)
      end

      def enqueue_all(jobs)
        enqueued_count = 0
        jobs.group_by(&:queue_name).each do |queue_name, same_queue_jobs|
          enqueued_count += enqueue_batches(queue_name, same_queue_jobs)
        end
        enqueued_count
      end

      private

      def enqueue_batches(queue_name, same_queue_jobs)
        enqueued_count = 0
        queue_url = Aws::ActiveJob::SQS.config.url_for(queue_name)

        same_queue_jobs.each_slice(10) do |chunk|
          enqueued_count += enqueue_batch(queue_url, chunk)
        end
        enqueued_count
      end

      def enqueue_batch(queue_url, chunk)
        entries = chunk.map do |job|
          entry = Params.new(job, nil).entry
          entry[:id] = job.job_id
          entry[:delay_seconds] = Params.assured_delay_seconds(job.scheduled_at) if job.scheduled_at
          entry
        end

        send_message_opts = {
          queue_url: queue_url,
          entries: entries
        }

        send_message_batch_result = Aws::ActiveJob::SQS.config.client.send_message_batch(send_message_opts)
        send_message_batch_result.successful.count
      end

      def _enqueue(job, body = nil, send_message_opts = {})
        body ||= job.serialize
        params = Params.new(job, body)
        send_message_opts = send_message_opts.merge(params.entry)
        send_message_opts[:queue_url] = params.queue_url
        response = Aws::ActiveJob::SQS.config.client.send_message(send_message_opts)
        job.provider_job_id = response.message_id
      end
    end
  end
end
