# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      # Raised when a message names a job class that cannot be executed: the
      # name is not a well-formed constant path, it is not in the configured
      # job_class_allowlist, it is undefined, or it does not name a class
      # inheriting from ActiveJob::Base. This is a permanent
      # failure: such a message can never succeed, so the executor logs and
      # deletes it rather than letting it redeliver. A dedicated type (rather
      # than a generic ArgumentError) keeps it distinct from errors raised from
      # inside a job's own #perform, which remain retryable.
      class InvalidJobClassError < StandardError; end

      # @api private
      class JobRunner
        # A job class name is an (optionally namespaced) constant path and
        # nothing else. Names that cannot match this can never name a job
        # class, so rejecting them keeps them out of the constant lookup.
        JOB_CLASS_NAME_PATTERN = /\A[A-Z]\w*(::[A-Z]\w*)*\z/.freeze

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

        # Checks are ordered so that the least trusting one runs first. Merely
        # resolving a constant is not inert: in Rails it triggers autoloading,
        # which runs the body of whichever file defines it. So the name is
        # matched against the allowlist as a String, before any lookup, and an
        # excluded name never reaches the constant resolver at all.
        def resolve_job_class(name)
          name = name.to_s
          unless JOB_CLASS_NAME_PATTERN.match?(name)
            raise InvalidJobClassError, "#{name.inspect} is not a valid job class name"
          end

          allowlist = normalized_allowlist
          if allowlist && !allowlist.include?(name)
            raise InvalidJobClassError, "#{name} is not in the configured job_class_allowlist"
          end

          klass = constantize_job_class(name)
          unless klass.is_a?(Class) && klass < ::ActiveJob::Base
            raise InvalidJobClassError, "#{name} is not a valid job class (must inherit from ActiveJob::Base)"
          end

          klass
        end

        # Resolves an already-validated name. +inherit+ is false so that each
        # segment must be defined directly on the preceding one: a name such
        # as 'SomeJob::Foo' cannot resolve Foo from SomeJob's ancestors when
        # SomeJob itself does not define it. NameError is treated as a
        # rejection rather than propagated: an unknown class is the same
        # permanent failure as a disallowed one.
        def constantize_job_class(name)
          Object.const_get(name, false)
        rescue NameError
          raise InvalidJobClassError, "#{name} is not defined"
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
