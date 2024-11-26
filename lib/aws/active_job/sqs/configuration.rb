# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # Use +Aws::ActiveJob::SQS.config+ to access the singleton config instance.
      class Configuration
        # Default configuration options
        # @api private
        DEFAULTS = {
          threads: 2 * Concurrent.processor_count,
          backpressure: 10,
          max_messages: 10,
          shutdown_timeout: 15,
          queues: {},
          logger: ::Rails.logger,
          message_group_id: 'SqsActiveJobGroup',
          excluded_deduplication_keys: ['job_id']
        }.freeze

        GLOBAL_ENV_CONFIGS = %i[
          threads backpressure
          max_messages shutdown_timeout
          visibility_timeout message_group_id
        ].freeze

        QUEUE_ENV_CONFIGS = %i[
          url max_messages
          visibility_timeout message_group_id
        ].freeze

        # Don't use this method directly: Configuration is a singleton class, use
        # +Aws::ActiveJob::SQS.config+ to access the singleton config.
        #
        # This class provides a Configuration object for AWS ActiveJob
        # by pulling configuration options from Runtime, the ENV, a YAML file,
        # and default settings, in that order. Values set on queues are used
        # preferentially to global values.
        #
        # # Environment Variables
        # The Configuration loads global and queue specific values from your
        # environment. Global keys take the form of:
        # `AWS_ACTIVE_JOB_SQS_<KEY_NAME>` and queue specific keys take the
        # form of: `AWS_ACTIVE_JOB_SQS_<QUEUE_NAME>_<KEY_NAME>`. Example:
        #
        #     export AWS_ACTIVE_JOB_SQS_MAX_MESSAGES = 5
        #     export AWS_ACTIVE_JOB_SQS_DEFAULT_URL = https://my-queue.aws
        #
        # @param [Hash] options
        # @option options [Hash[Symbol, Hash]] :queues A mapping between the
        #   active job queue name and the queue properties. Valid properties
        #   are: url [Required], max_messages, shutdown_timeout,
        #   message_group_id, and :excluded_deduplication_keys. Values
        #   configured on the queue are used preferentially to the global
        #   values.
        #   Note: multiple active job queues can map to the same SQS Queue URL.
        #
        # @option options  [Integer] :max_messages
        #    The max number of messages to poll for in a batch.
        #
        # @option options [Integer] :visibility_timeout
        #   If unset, the visibility timeout configured on the
        #   SQS queue will be used.
        #   The visibility timeout is the number of seconds
        #   that a message will not be processable by any other consumers.
        #   You should set this value to be longer than your expected job runtime
        #   to prevent other processes from picking up an running job.
        #   See the (SQS Visibility Timeout Documentation)[https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html]
        #
        # @option options [Integer] :shutdown_timeout
        #   the amount of time to wait
        #   for a clean shutdown.  Jobs that are unable to complete in this time
        #   will not be deleted from the SQS queue and will be retryable after
        #   the visibility timeout.
        #
        # @option options [Callable] :poller_error_handler and error handler to
        #   be called when the poller encounters an error running a job.  Called
        #   with exception, sqs_message. You may re-raise the exception to
        #   terminate the poller. You may also choose whether to delete the
        #   sqs_message or not.  If the message is not explicitly deleted
        #   then the message will be left on the queue and will be
        #   retried (pending the SQS Queue's redrive/DLQ/maximum receive settings).
        #   Retries provided by this mechnaism be on top of any retries
        #   configured on the job with `retry_on`.
        #
        #   This method is only for errors not handled by ActiveJob's
        #   [Retry/Discard handling](https://guides.rubyonrails.org/active_job_basics.html#retrying-or-discarding-failed-jobs)
        #
        # @option options [ActiveSupport::Logger] :logger Logger to use
        #   for the poller.
        #
        # @option options [String] :config_file
        #   Override file to load configuration from. If not specified will
        #   attempt to load from config/aws_sqs_active_job.yml.
        #
        # @option options [String] :message_group_id (SqsActiveJobGroup)
        #  The message_group_id to use for queueing messages on a fifo queues.
        #  Applies only to jobs queued on FIFO queues.
        #  See the (SQS FIFO Documentation)[https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html]
        #
        # @option options [Callable] :async_queue_error_handler An error handler
        #   to be called when the async active job adapter experiences an error
        #   queueing a job.  Only applies when
        #   +active_job.queue_adapter = :sqs_async+.  Called with:
        #   [error, job, job_options]
        #
        # @option options [SQS::Client] :client SQS Client to use. A default
        #   client will be created if none is provided.
        #
        # @option options [Array] :excluded_deduplication_keys (['job_id'])
        #   The type of keys stored in the array should be String or Symbol.
        #   Using this option, job_id is implicitly added to the keys.

        def initialize(options = {})
          options[:config_file] ||= config_file if File.exist?(config_file)
          resolved = DEFAULTS
                    .merge(file_options(options))

          resolved = resolved
                    .merge(env_options(resolved))
                    .merge(options)
          set_attributes(resolved)
        end

        # @api private
        attr_accessor :queues, :threads, :backpressure,
                      :max_messages, :visibility_timeout,
                      :shutdown_timeout, :client, :logger,
                      :async_queue_error_handler, :message_group_id,
                      :poller_error_handler

        attr_reader :excluded_deduplication_keys

        def excluded_deduplication_keys=(keys)
          @excluded_deduplication_keys = keys.map(&:to_s) | ['job_id']
        end

        def client
          @client ||= begin
            client = Aws::SQS::Client.new
            client.config.user_agent_frameworks << 'aws-activejob-sqs'
            client
          end
        end

        # Return the queue_url for a given job_queue name
        def url_for(job_queue)
          queue_attribute_for(:url, job_queue)
        end

        def max_messages_for(job_queue)
          queue_attribute_for(:max_messages, job_queue)
        end

        def visibility_timeout_for(job_queue)
          queue_attribute_for(:visibility_timeout, job_queue)
        end

        def message_group_id_for(job_queue)
          queue_attribute_for(:message_group_id, job_queue)
        end

        def excluded_deduplication_keys_for(job_queue)
          queue_attribute_for(:excluded_deduplication_keys, job_queue)
        end

        # @api private
        def to_s
          to_h.to_s
        end

        # @api private
        def to_h
          h = {}
          instance_variables.each do |v|
            v_sym = v.to_s.delete('@').to_sym
            val = instance_variable_get(v)
            h[v_sym] = val
          end
          h
        end

        private

        def queue_attribute_for(attribute, job_queue)
          job_queue = job_queue.to_sym
          raise ArgumentError, "No queue defined for #{job_queue}" unless queues.key? job_queue

          queues[job_queue][attribute] || instance_variable_get("@#{attribute}")
        end

        # Set accessible attributes after merged options.
        def set_attributes(options)
          options.each_key do |opt_name|
            instance_variable_set("@#{opt_name}", options[opt_name])
            client.config.user_agent_frameworks << 'aws-activejob-sqs' if opt_name == :client
          end
        end

        # resolve ENV for global and queue specific options
        def env_options(options)
          resolved = {}
          GLOBAL_ENV_CONFIGS.each do |cfg|
            env_name = "AWS_ACTIVE_JOB_SQS_#{cfg.to_s.upcase}"
            resolved[cfg] = parse_env_value(env_name) if ENV.key? env_name
          end
          options[:queues]&.each_key do |queue|
            resolved[:queues] ||= {}
            resolved[:queues][queue] = options[:queues][queue].dup
            QUEUE_ENV_CONFIGS.each do |cfg|
              env_name = "AWS_ACTIVE_JOB_SQS_#{queue.upcase}_#{cfg.to_s.upcase}"
              resolved[:queues][queue][cfg] = parse_env_value(env_name) if ENV.key? env_name
            end
          end
          resolved
        end

        def parse_env_value(key)
          val = ENV.fetch(key, nil)
          Integer(val)
        rescue ArgumentError, TypeError
          %w[true false].include?(val) ? val == 'true' : val
        end

        def file_options(options = {})
          file_path = config_file_path(options)
          if file_path
            load_from_file(file_path)
          else
            options
          end
        end

        def config_file
          file = ::Rails.root.join("config/aws_sqs_active_job/#{::Rails.env}.yml")
          file = ::Rails.root.join('config/aws_sqs_active_job.yml') unless File.exist?(file)
          file
        end

        # Load options from YAML file
        def load_from_file(file_path)
          opts = load_yaml(file_path) || {}
          opts[:queues]&.each_key do |queue|
            if opts[:queues][queue].is_a?(String)
              opts[:queues][queue] = { url: opts[:queues][queue] }
            end
          end
          opts.deep_symbolize_keys
        end

        # @return [String] Configuration path found in environment or YAML file.
        def config_file_path(options)
          options[:config_file] || ENV.fetch('AWS_ACTIVE_JOB_SQS_CONFIG_FILE', nil)
        end

        def load_yaml(file_path)
          require 'erb'
          source = ERB.new(File.read(file_path)).result

          # Avoid incompatible changes with Psych 4.0.0
          # https://bugs.ruby-lang.org/issues/17866
          begin
            YAML.safe_load(source, aliases: true) || {}
          rescue ArgumentError
            YAML.safe_load(source) || {}
          end
        end
      end
    end
  end
end
