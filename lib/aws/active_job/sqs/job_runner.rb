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
          klass = name.constantize
          unless klass.is_a?(Class) && klass < ::ActiveJob::Base
            raise ArgumentError, "#{name} is not a valid job class (must inherit from ActiveJob::Base)"
          end
          allowlist = Aws::ActiveJob::SQS.config.job_class_allowlist
          if allowlist && !allowlist.include?(klass)
            raise ArgumentError, "#{name} is not in the configured job_class_allowlist"
          end
          klass
        end
      end
    end
  end
end
