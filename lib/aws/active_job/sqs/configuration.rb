# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # This class provides a Configuration object for AWS ActiveJob
      # by pulling configuration options from runtime code, the ENV, a YAML file,
      # and default settings, in that order. Values set on queues are used
      # preferentially to global values.
      #
      # Use {Aws::ActiveJob::SQS.config Aws::ActiveJob::SQS.config}
      # to access the singleton config instance and use
      # {Aws::ActiveJob::SQS.configure Aws::ActiveJob::SQS.configure} to
      # configure in code:
      #
      #     Aws::ActiveJob::SQS.configure do |config|
      #       config.logger = Rails.logger
      #       config.max_messages = 5
      #     end
      #
      # # Configuation YAML File
      # By default, this class will load configuration from the
      # `config/aws_active_job_sqs/<RAILS_ENV}.yml` or
      # `config/aws_active_job_sqs.yml` files.  You may specify the file used
      # through the `:config_file` option in code or the
      # `AWS_ACTIVE_JOB_SQS_CONFIG_FILE` environment variable.
      #
      # # Global and queue specific options
      # Values configured for specific queues are used preferentially to
      # global values. See: {QUEUE_CONFIGS} for supported queue specific
      # options.
      #
      # # Environment Variables
      # The Configuration loads global and qubeue specific values from your
      # environment. Global keys take the form of:
      # `AWS_ACTIVE_JOB_SQS_<KEY_NAME>` and queue specific keys take the
      # form of: `AWS_ACTIVE_JOB_SQS_<QUEUE_NAME>_<KEY_NAME>`.
      # <QUEUE_NAME> is case-insensitive and is always down cased. Configuring
      # non-snake case queues (containing upper case) through ENV is
      # not supported.
      #
      # Example:
      #
      #     export AWS_ACTIVE_JOB_SQS_MAX_MESSAGES = 5
      #     export AWS_ACTIVE_JOB_SQS_DEFAULT_URL = https://my-queue.aws
      #
      # For supported global ENV configurations see
      # {GLOBAL_ENV_CONFIGS}.  For supported queue specific ENV configurations
      # see: {QUEUE_ENV_CONFIGS}.
      #
      class Configuration
        # Default configuration options
        # @api private
        DEFAULTS = {
          threads: 2 * Concurrent.processor_count,
          backpressure: 10,
          max_messages: 10,
          shutdown_timeout: 15,
          queues: Hash.new { |h, k| h[k] = {} },
          message_group_id: 'ActiveJobSqsGroup',
          excluded_deduplication_keys: ['job_id']
        }.freeze

        GLOBAL_ENV_CONFIGS = %i[
          config_file
          threads
          backpressure
          max_messages
          shutdown_timeout
          visibility_timeout
          message_group_id
        ].freeze

        QUEUE_ENV_CONFIGS = %i[
          url
          max_messages
          visibility_timeout
          message_group_id
        ].freeze

        QUEUE_CONFIGS = QUEUE_ENV_CONFIGS + %i[excluded_deduplication_keys]

        QUEUE_KEY_REGEX =
          /AWS_ACTIVE_JOB_SQS_([\w]+)_(#{QUEUE_ENV_CONFIGS.map(&:upcase).join('|')})/.freeze

        # Don't use this method directly: Configuration is a singleton class,
        # use {Aws::ActiveJob::SQS.config Aws::ActiveJob::SQS.config}
        # to access the singleton config instance and use
        # {Aws::ActiveJob::SQS.configure Aws::ActiveJob::SQS.configure} to
        # configure in code:
        #
        # @param [Hash] options
        # @option options [Hash<Symbol, Hash>] :queues A mapping between the
        #   active job queue name and the queue properties. Values
        #   configured on the queue are used preferentially to the global
        #   values. See: {QUEUE_CONFIGS} for supported queue specific options.
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
        #   retried (pending the SQS Queue's redrive/DLQ/maximum
        #   receive settings). Retries provided by this mechanism are
        #   after any retries configured on the job with `retry_on`.
        #
        # @option options [ActiveSupport::Logger] :logger Logger to use
        #   for the poller.
        #
        # @option options [String] :config_file
        #   Override file to load configuration from. If not specified will
        #   attempt to load from config/aws_active_job_sqs.yml.
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
          opts = env_options.deep_merge(options)
          opts = file_options(opts).deep_merge(opts)
          opts = DEFAULTS.merge(logger: default_logger).deep_merge(opts)

          apply_attributes(opts)
        end

        # @api private
        attr_accessor :queues, :threads, :backpressure,
                      :shutdown_timeout, :logger,
                      :async_queue_error_handler

        # @api private
        attr_writer :max_messages, :message_group_id, :visibility_timeout,
                    :poller_error_handler, :client

        def excluded_deduplication_keys=(keys)
          @excluded_deduplication_keys = keys.map(&:to_s) | ['job_id']
        end

        def poller_error_handler(&block)
          @poller_error_handler = block if block_given?
          @poller_error_handler
        end

        def client
          @client ||= begin
            client = Aws::SQS::Client.new
            client.config.user_agent_frameworks << 'aws-activejob-sqs'
            client
          end
        end

        QUEUE_CONFIGS.each do |key|
          define_method(:"#{key}_for") do |job_queue|
            queue_attribute_for(key, job_queue)
          end
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
        def apply_attributes(options)
          options.each_key do |opt_name|
            instance_variable_set("@#{opt_name}", options[opt_name])
            client.config.user_agent_frameworks << 'aws-activejob-sqs' if opt_name == :client
          end
        end

        # resolve ENV for global and queue specific options
        def env_options
          resolved = { queues: {} }
          resolve_global_env_options(resolved)
          resolve_queue_env__options(resolved)
          resolved
        end

        def resolve_queue_env__options(resolved)
          ENV.each_key do |key|
            next unless (match = QUEUE_KEY_REGEX.match(key))

            queue_name = match[1].downcase.to_sym
            resolved[:queues][queue_name] ||= {}
            resolved[:queues][queue_name][match[2].downcase.to_sym] =
              parse_env_value(key)
          end
        end

        def resolve_global_env_options(resolved)
          GLOBAL_ENV_CONFIGS.each do |cfg|
            env_name = "AWS_ACTIVE_JOB_SQS_#{cfg.to_s.upcase}"
            resolved[cfg] = parse_env_value(env_name) if ENV.key? env_name
          end
        end

        def parse_env_value(key)
          val = ENV.fetch(key, nil)
          Integer(val)
        rescue ArgumentError, TypeError
          %w[true false].include?(val) ? val == 'true' : val
        end

        def file_options(options = {})
          file_path = options[:config_file] || default_config_file
          if file_path
            load_from_file(file_path)
          else
            options
          end
        end

        def default_config_file
          return unless defined?(::Rails)

          file = ::Rails.root.join("config/aws_active_job_sqs/#{::Rails.env}.yml")
          file = ::Rails.root.join('config/aws_active_job_sqs.yml') unless File.exist?(file)
          file
        end

        # Load options from YAML file
        def load_from_file(file_path)
          opts = load_yaml(file_path) || {}
          opts = opts.deep_symbolize_keys
          opts[:queues]&.each_key do |queue|
            opts[:queues][queue] = { url: opts[:queues][queue] } if opts[:queues][queue].is_a?(String)
          end
          opts
        end

        def load_yaml(file_path)
          return {} unless File.exist?(file_path)

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

        def default_logger
          if defined?(::Rails)
            ::Rails.logger
          else
            ActiveSupport::Logger.new($stdout)
          end
        end
      end
    end
  end
end
