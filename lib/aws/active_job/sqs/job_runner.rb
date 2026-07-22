# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # @api private
      class JobRunner
        attr_reader :id, :class_name

        def initialize(message)
          @job_data = ActiveSupport::JSON.load(message.data.body)
          @class_name = resolve_job_class(@job_data['job_class'])
          @id = @job_data['job_id']
        end

        def run
          ::ActiveJob::Base.execute @job_data
        end

        def exception_executions?
          @job_data['exception_executions'] &&
            !@job_data['exception_executions'].empty?
        end

        private

        def resolve_job_class(name)
          klass = name.safe_constantize
          unless klass.is_a?(Class) && klass < ::ActiveJob::Base
            raise ArgumentError, "#{name} is not a valid job class (must inherit from ActiveJob::Base)"
          end

          allowlist = normalized_allowlist
          return klass if allowlist.nil? || allowlist.include?(klass.name)

          raise ArgumentError, "#{name} is not in the configured job_class_allowlist"
        end

        # The allowlist may be configured as an Array of Class values (in code),
        # an Array of Strings (from the YAML file), or a comma-separated String
        # (from ENV). Normalize all forms to an Array of class name Strings.
        def normalized_allowlist
          allowlist = Aws::ActiveJob::SQS.config.job_class_allowlist
          allowlist = allowlist.split(',') if allowlist.is_a?(String)
          allowlist&.map { |entry| entry.to_s.strip }
        end
      end
    end
  end
end
