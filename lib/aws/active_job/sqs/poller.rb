# frozen_string_literal: true

require 'aws-sdk-sqs'
require 'optparse'
require 'concurrent'

module Aws
  module ActiveJob
    module SQS
      # CLI runner for polling for SQS ActiveJobs
      # Use `aws_sqs_active_job --help` for detailed usage
      class Poller
        class Interrupt < StandardError; end

        def initialize(args = ARGV)
          @options = parse_args(args)
          # Set_environment must be run before we boot_rails
          set_environment
        end

        def set_environment
          @environment = @options[:environment] || ENV['APP_ENV'] || ENV['RAILS_ENV'] || ENV['RACK_ENV'] || 'development'
        end

        def run
          # exit 0
          boot_rails

          # cannot load config (from file or initializers) until after
          # rails has been booted.\
          Aws::ActiveJob::SQS.configure do |cfg|
            @options.each_pair do |key, value|
              cfg.send(:"#{key}=", value) if cfg.respond_to?(:"#{key}=")
            end
          end

          validate_config

          config = Aws::ActiveJob::SQS.config

          # ensure we have a logger configured
          @logger = config.logger || ActiveSupport::Logger.new($stdout)
          @logger.info("Starting Poller with config=#{config.to_h}")

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
          shutdown
          exit
        end

        private

        def shutdown
          @executor.shutdown(Aws::ActiveJob::SQS.config.shutdown_timeout)
        end

        def poll
          config = Aws::ActiveJob::SQS.config
          queue = @options[:queue]
          queue_url = config.url_for(queue)
          client = config.client
          @poller = Aws::SQS::QueuePoller.new(queue_url, client: client)
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

          single_message = poller_options[:max_number_of_messages] == 1

          @logger.info "Polling on: #{queue} => #{queue_url} with options=#{poller_options}"

          @poller.poll(poller_options) do |msgs|
            msgs = [msgs] if single_message
            @logger.info "Processing batch of #{msgs.length} messages"
            msgs.each do |msg|
              @executor.execute(Aws::SQS::Message.new(
                                  queue_url: queue_url,
                                  receipt_handle: msg.receipt_handle,
                                  data: msg,
                                  client: client
                                ))
            end
          end
        end

        def boot_rails
          ENV['RACK_ENV'] = ENV['RAILS_ENV'] = @environment
          require 'rails'
          require File.expand_path('config/environment.rb')
        end

        # rubocop:disable Metrics
        def parse_args(argv)
          out = {}
          parser = ::OptionParser.new do |opts|
            opts.on('-q', '--queue STRING', '[Required] Queue to poll') { |a| out[:queue] = a }
            opts.on('-e', '--environment STRING',
                    'Rails environment (defaults to development). You can also use the APP_ENV or RAILS_ENV environment variables to specify the environment.') do |a|
              out[:environment] = a
            end
            opts.on('-t', '--threads INTEGER', Integer,
                    'The maximum number of worker threads to create.  Defaults to 2x the number of processors available on this system.') do |a|
              out[:threads] = a
            end
            opts.on('-b', '--backpressure INTEGER', Integer,
                    'The maximum number of messages to have waiting in the Executor queue. This should be a low, but non zero number.  Messages in the Executor queue cannot be picked up by other processes and will slow down shutdown.') do |a|
              out[:backpressure] = a
            end
            opts.on('-m', '--max_messages INTEGER', Integer,
                    'Max number of messages to receive in a batch from SQS.') do |a|
              out[:max_messages] = a
            end
            opts.on('-v', '--visibility_timeout INTEGER', Integer,
                    'The visibility timeout is the number of seconds that a message will not be processable by any other consumers. You should set this value to be longer than your expected job runtime to prevent other processes from picking up an running job.  See the SQS Visibility Timeout Documentation at https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html.') do |a|
              out[:visibility_timeout] = a
            end
            opts.on('-s', '--shutdown_timeout INTEGER', Integer,
                    'The amount of time to wait for a clean shutdown.  Jobs that are unable to complete in this time will not be deleted from the SQS queue and will be retryable after the visibility timeout.') do |a|
              out[:shutdown_timeout] = a
            end
          end

          parser.banner = 'aws_sqs_active_job [options]'
          parser.on_tail '-h', '--help', 'Show help' do
            puts parser
            exit 1
          end

          parser.parse(argv)
          out
        end
        # rubocop:enable Metrics

        def validate_config
          raise ArgumentError, 'You must specify the name of the queue to process jobs from' unless @options[:queue]
        end
      end
    end
  end
end
