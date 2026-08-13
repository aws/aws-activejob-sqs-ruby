# frozen_string_literal: true

require 'concurrent'

module Aws
  module ActiveJob
    module SQS
      # CLI runner for polling for SQS ActiveJobs
      class Executor
        DEFAULTS = {
          min_threads: 0,
          max_threads: Integer(Concurrent.available_processor_count || Concurrent.processor_count),
          auto_terminate: true,
          idletime: 60, # 1 minute
          fallback_policy: :abort # Concurrent::RejectedExecutionError must be handled
        }.freeze

        class << self
          def on_stop(&block)
            lifecycle_hooks[:stop] << block
          end

          def lifecycle_hooks
            @lifecycle_hooks ||= Hash.new { |h, k| h[k] = [] }
          end

          def clear_hooks
            @lifecycle_hooks = nil
          end
        end

        def initialize(options = {})
          @executor = Concurrent::ThreadPoolExecutor.new(DEFAULTS.merge(options))
          @logger = options[:logger] || ActiveSupport::Logger.new($stdout)
          @task_complete = Concurrent::Event.new
          @post_mutex = Mutex.new

          @error_handler = options[:error_handler]
          @error_queue = Thread::Queue.new
          @error_handler_thread = Thread.new(&method(:handle_errors))
          @error_handler_thread.abort_on_exception = true
          @error_handler_thread.report_on_exception = false
          @shutting_down = Concurrent::AtomicBoolean.new(false)
        end

        def execute(message)
          @post_mutex.synchronize do
            _execute(message)
          end
        end

        def shutdown(timeout = nil)
          @shutting_down.make_true

          run_hooks_for(:stop)
          @executor.shutdown
          clean_shutdown = @executor.wait_for_termination(timeout)
          if clean_shutdown
            @logger.info 'Clean shutdown complete.  All executing jobs finished.'
          else
            @logger.info "Timeout (#{timeout}) exceeded.  Some jobs may not have " \
                         'finished cleanly.  Unfinished jobs will not be removed from ' \
                         'the queue and can be ru-run once their visibility timeout ' \
                         'passes.'
          end
          @error_queue.push(nil) # process any remaining errors and then terminate
          @error_handler_thread.join unless @error_handler_thread == Thread.current
          @shutting_down.make_false
        end

        private

        def _execute(message)
          post_task(message)
        rescue Concurrent::RejectedExecutionError
          # no capacity, wait for a task to complete
          @task_complete.reset
          @task_complete.wait
          retry
        end

        def post_task(message)
          @executor.post(message) do |msg|
            execute_task(msg)
          end
        end

        def execute_task(message)
          job = JobRunner.new(message)
          @logger.info("Running job: #{job.id}[#{job.class_name}]")
          job.run
          message.delete
        rescue JSON::ParserError => e
          # An unparseable body is a permanent failure: the message content is
          # immutable, so re-parsing it on redelivery can never succeed. Log and
          # delete it so it does not redeliver indefinitely.
          parser_msg = "Unable to parse message body: #{message.data.body}. Error: #{e}."
          drop_permanent_failure(message, e, parser_msg)
        rescue InvalidJobClassError => e
          # A bad job class is a permanent failure: redelivering it can never
          # succeed and, on the default config, would crash-loop the poller.
          # Log the forensic detail (the message can't be inspected in a DLQ
          # once deleted) and delete it so it does not redeliver.
          invalid_msg =
            "Rejecting message #{message.message_id}: #{e}. " \
            "Body: #{message.data.body}. Deleting so it does not redeliver."
          drop_permanent_failure(message, e, invalid_msg)
        rescue StandardError => e
          handle_standard_error(e, job, message)
        ensure
          @task_complete.set
        end

        # Handle a permanently-failed message (unparseable body or invalid job
        # class). Always logged. A dropped message is also reported to the
        # error tracker so the loss is visible beyond the logs, then deleted so
        # SQS won't redeliver. A configured +permanent_failure_handler+ can
        # +throw :skip_delete+ to keep the message on the queue for redelivery
        # instead, in which case it is neither reported nor deleted.
        def drop_permanent_failure(message, error, log_message)
          @logger.error log_message

          handler = Aws::ActiveJob::SQS.config.permanent_failure_handler
          unless handler
            report_permanent_failure(error, message)
            return message.delete
          end

          catch(:skip_delete) do
            begin
              handler.call(error, message)
            rescue StandardError => e
              @logger.error "permanent_failure_handler raised: #{e}"
            end
            report_permanent_failure(error, message)
            message.delete
          end
        end

        # Report a permanently-dropped job to the application's error tracker
        # via Rails' error reporter (Rails 7+), if available, so the loss
        # surfaces in whatever tracker the app uses. A no-op outside Rails.
        # Only called when the message is actually deleted, so a message kept
        # for redelivery via +:skip_delete+ is not reported as dropped.
        def report_permanent_failure(error, message)
          return unless defined?(::Rails) && ::Rails.respond_to?(:error)

          ::Rails.error.report(
            error,
            handled: true,
            context: { message_id: message.message_id, body: message.data.body }
          )
        end

        def handle_standard_error(error, job, message)
          job_msg = job ? "#{job.id}[#{job.class_name}]" : 'unknown job'
          @logger.info "Error processing job #{job_msg}: #{error}"
          @logger.debug error.backtrace.join("\n")

          @error_queue.push([error, message])
        end

        def run_hooks_for(event_name)
          return unless (hooks = self.class.lifecycle_hooks[event_name])

          hooks.each(&:call)
        end

        # run in the @error_handler_thread
        def handle_errors
          # wait until errors are placed in the error queue
          while ((exception, message) = @error_queue.pop)
            raise exception unless @error_handler

            @error_handler.call(exception, message)

          end
        rescue StandardError => e
          @logger.info("Unhandled exception executing jobs in poller: #{e}.")
          @logger.info('Shutting down executor')
          shutdown unless @shutting_down.true?

          raise e # re-raise the error, terminating the application
        end
      end
    end
  end
end
