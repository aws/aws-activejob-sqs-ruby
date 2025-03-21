# frozen_string_literal: true

require 'aws-sdk-sqs'
require 'concurrent'

module Aws
  module ActiveJob
    module SQS
      # CLI runner for polling for SQS ActiveJobs
      # Use `aws_active_job_sqs --help` for detailed usage
      class Poller
        class Interrupt < StandardError; end

        def initialize(options = {})
          @queues = options.delete(:queues)
          @options = options
        end

        def run
          init_config

          config = Aws::ActiveJob::SQS.config

          Signal.trap('INT') { raise Interrupt }
          Signal.trap('TERM') { raise Interrupt }
          @executor = Executor.new(
            max_threads: config.threads,
            logger: @logger,
            max_queue: config.backpressure,
            error_handler: config.poller_error_handler
          )

          poll
        rescue Interrupt
          @logger.info 'Process Interrupted or killed - attempting to shutdown cleanly.'
          shutdown(config.shutdown_timeout)
          exit
        end

        private

        def init_config
          Aws::ActiveJob::SQS.configure do |cfg|
            @options.each_pair do |key, value|
              cfg.send(:"#{key}=", value) if cfg.respond_to?(:"#{key}=")
            end
          end

          # ensure we have a logger configured
          config = Aws::ActiveJob::SQS.config
          @logger = config.logger || ActiveSupport::Logger.new($stdout)
          @logger.info("Starting Poller with config=#{config.to_h}")
        end

        def shutdown(timeout)
          @executor.shutdown(timeout)
        end

        def poll
          config = Aws::ActiveJob::SQS.config
          if @queues && !@queues.empty?
            if @queues.size == 1
              # single queue, use main thread
              poll_foreground(@queues.first)
            else
              poll_background(@queues)
            end
          else
            # poll on all configured queues
            @logger.info("No queues specified - polling on all configured queues: #{config.queues.keys}")
            poll_background(config.queues.keys)
          end
        end

        def poll_foreground(queue)
          config = Aws::ActiveJob::SQS.config
          validate_config(queue)
          queue_url = config.url_for(queue)

          poller_options = poller_options(queue)
          @logger.info "Foreground Polling on: #{queue} => #{queue_url} with options=#{poller_options}"

          _poll(poller_options, queue_url)
        end

        def poll_background(queues)
          config = Aws::ActiveJob::SQS.config
          queues.each { |q| validate_config(q) }
          poller_threads = queues.map do |queue|
            Thread.new do
              queue_url = config.url_for(queue)

              poller_options = poller_options(queue)
              @logger.info "Background Polling on: #{queue} => #{queue_url} with options=#{poller_options}"

              _poll(poller_options, queue_url)
            end
          end
          poller_threads.each(&:join)
        end

        def validate_config(queue)
          return if Aws::ActiveJob::SQS.config.queues[queue]&.fetch(:url, nil)

          raise ArgumentError, "No URL configured for queue #{queue}"
        end

        def poller_options(queue)
          config = Aws::ActiveJob::SQS.config
          queue_url = config.url_for(queue)
          poller_options = {
            skip_delete: true,
            max_number_of_messages: config.max_messages_for(queue),
            visibility_timeout: config.visibility_timeout_for(queue)
          }

          # Limit max_number_of_messages for FIFO queues to 1
          # this ensures jobs with the same message_group_id are processed
          # in order
          # Jobs with different message_group_id will be processed in
          # parallel and may be out of order.
          poller_options[:max_number_of_messages] = 1 if Aws::ActiveJob::SQS.fifo?(queue_url)
          poller_options
        end

        def _poll(poller_options, queue_url)
          poller = Aws::SQS::QueuePoller.new(
            queue_url,
            client: Aws::ActiveJob::SQS.config.client
          )
          single_message = poller_options[:max_number_of_messages] == 1
          poller.poll(poller_options) do |msgs|
            msgs = [msgs] if single_message
            execute_messages(msgs, queue_url)
          end
        end

        def execute_messages(msgs, queue_url)
          @logger.info "Processing batch of #{msgs.length} messages"
          msgs.each do |msg|
            sqs_message = Aws::SQS::Message.new(
              queue_url: queue_url,
              receipt_handle: msg.receipt_handle,
              data: msg,
              client: Aws::ActiveJob::SQS.config.client
            )
            @executor.execute(sqs_message)
          end
        end
      end
    end
  end
end
