# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe Executor do
        let(:logger) { double(info: nil, debug: nil, error: nil) }

        before do
          allow(ActiveSupport::Logger).to receive(:new).and_return(logger)
        end

        it 'merges runtime options with defaults' do
          expected = Executor::DEFAULTS.merge(max_queue: 10)
          expect(Concurrent::ThreadPoolExecutor).to receive(:new).with(expected)
          Executor.new(max_queue: 10)
        end

        describe '#execute' do
          let(:body) { ActiveSupport::JSON.dump(TestJob.new('a1', 'a2').serialize) }
          # message is a reserved minitest name
          let(:msg) { double(data: double(body: body)) }
          let(:executor) { Executor.new }
          let(:runner) { double('runner', id: 'jobid', class_name: 'jobclass', exception_executions?: false) }

          it 'executes the job and deletes the message' do
            expect(JobRunner).to receive(:new).and_return(runner)
            expect(runner).to receive(:run)
            expect(msg).to receive(:delete)
            executor.execute(msg)
            executor.shutdown # give the job a chance to run
          end

          it 'raises the error and terminates poller' do
            expect(JobRunner).to receive(:new).and_return(runner)
            expect(runner).to receive(:run).and_raise StandardError
            expect do
              executor.execute(msg)
              sleep(0.1) if defined?(JRUBY_VERSION)
              executor.shutdown # give the job a chance to run
            end.to raise_exception(StandardError)
          end

          it 'deletes the message and does not re-raise when the job class is invalid' do
            expect(JobRunner).to receive(:new).and_raise(InvalidJobClassError, 'File is not a valid job class')
            expect(msg).to receive(:delete)
            allow(msg).to receive(:message_id).and_return('stub-id')
            expect do
              executor.execute(msg)
              sleep(0.1) if defined?(JRUBY_VERSION)
              executor.shutdown # give the task a chance to run
            end.not_to raise_error
          end

          it 'deletes the message and does not re-raise when the body cannot be parsed' do
            expect(JobRunner).to receive(:new).and_raise(JSON::ParserError.new('unexpected token'))
            expect(msg).to receive(:delete)
            allow(msg).to receive(:message_id).and_return('stub-id')
            expect do
              executor.execute(msg)
              sleep(0.1) if defined?(JRUBY_VERSION)
              executor.shutdown # give the task a chance to run
            end.not_to raise_error
          end

          describe 'permanent failures' do
            let(:error) { InvalidJobClassError.new('File is not a valid job class') }

            before do
              expect(JobRunner).to receive(:new).and_raise(error)
              allow(msg).to receive(:message_id).and_return('stub-id')
            end

            it 'reports the failure to the error tracker before deleting' do
              allow(msg).to receive(:delete)
              expect(Rails.error).to receive(:report).with(
                error,
                handled: true,
                context: { message_id: 'stub-id', body: body }
              )
              executor.execute(msg)
              sleep(0.1) if defined?(JRUBY_VERSION)
              executor.shutdown
            end

            context 'a permanent_failure_handler is configured' do
              after { Aws::ActiveJob::SQS.config.permanent_failure_handler = nil }

              it 'calls the handler with the error and message, then deletes' do
                handler = double
                expect(handler).to receive(:call).with(error, msg)
                Aws::ActiveJob::SQS.config.permanent_failure_handler = handler
                expect(msg).to receive(:delete)
                executor.execute(msg)
                sleep(0.1) if defined?(JRUBY_VERSION)
                executor.shutdown
              end

              it 'leaves the message on the queue when the handler throws :skip_delete' do
                Aws::ActiveJob::SQS.config.permanent_failure_handler =
                  ->(_error, _message) { throw :skip_delete }
                expect(msg).not_to receive(:delete)
                expect do
                  executor.execute(msg)
                  sleep(0.1) if defined?(JRUBY_VERSION)
                  executor.shutdown
                end.not_to raise_error
              end

              it 'does not report to the error tracker when the handler throws :skip_delete' do
                Aws::ActiveJob::SQS.config.permanent_failure_handler =
                  ->(_error, _message) { throw :skip_delete }
                expect(Rails.error).not_to receive(:report)
                expect(msg).not_to receive(:delete)
                executor.execute(msg)
                sleep(0.1) if defined?(JRUBY_VERSION)
                executor.shutdown
              end

              it 'deletes the message when the handler itself raises' do
                Aws::ActiveJob::SQS.config.permanent_failure_handler =
                  ->(_error, _message) { raise 'handler boom' }
                expect(msg).to receive(:delete)
                expect do
                  executor.execute(msg)
                  sleep(0.1) if defined?(JRUBY_VERSION)
                  executor.shutdown
                end.not_to raise_error
              end
            end
          end

          describe 'error_handler' do
            let(:error_handler) { double }
            let(:executor) { Executor.new(error_handler: error_handler) }
            let(:exception) { StandardError.new }

            it 'calls the error handler with exception and message' do
              expect(JobRunner).to receive(:new).at_least(:once).and_return(runner)
              expect(runner).to receive(:run).at_least(:once).and_raise exception
              expect(error_handler).to receive(:call).with(exception, msg)
              expect(executor).to receive(:shutdown).exactly(1).times.and_call_original

              executor.execute(msg)
              sleep(0.1) if defined?(JRUBY_VERSION)
              executor.shutdown # give the job a chance to run
            end
          end

          describe 'backpressure' do
            let(:executor) { Executor.new(max_threads: 1, max_queue: 1) }
            let(:trigger) { Concurrent::Event.new }

            it 'waits for a tasks to complete before attempting to post new tasks' do
              task_complete_event = executor.instance_variable_get(:@task_complete)
              expect(JobRunner).to receive(:new).at_least(:once).and_return(runner)
              allow(msg).to receive(:delete)
              allow(runner).to receive(:run) do
                trigger.wait
              end
              expect(task_complete_event).to receive(:wait).at_least(:once) do
                trigger.set # unblock the task
              end
              executor.execute(msg) # first message runs
              executor.execute(msg) # second message enters queue
              executor.execute(msg) # third message triggers wait
              executor.shutdown # wait for tasks to finish so none leak into other examples
            end
          end
        end

        describe '#shutdown' do
          let(:tp) { double }

          it 'calls shutdown and waits for termination' do
            expect(Concurrent::ThreadPoolExecutor).to receive(:new).and_return(tp)
            executor = Executor.new
            expect(tp).to receive(:shutdown)
            expect(tp).to receive(:wait_for_termination).with(5).and_return true
            executor.shutdown(5)
          end

          context 'errors during shutdown' do
            let(:error_handler) { double }
            let(:body) { ActiveSupport::JSON.dump(TestJob.new('a1', 'a2').serialize) }
            let(:msg) { double(data: double(body: body)) }
            let(:executor) { Executor.new(error_handler: error_handler) }
            let(:runner) { double('runner', id: 'jobid', class_name: 'jobclass', exception_executions?: false) }

            it 'handles errors from jobs during shutdown' do
              expect(JobRunner).to receive(:new).and_return(runner)
              expect(runner).to receive(:run) do
                sleep(0.1)
                raise StandardError
              end
              expect(error_handler).to receive(:call)
              expect(executor).to receive(:shutdown).exactly(1).times.and_call_original

              executor.execute(msg)
              sleep(0.1) if defined?(JRUBY_VERSION)
              executor.shutdown
            end
          end

          context 'lifecycle hooks are registered' do
            let(:hook) { double }

            before do
              allow(hook).to receive(:call)
            end

            after do
              Executor.clear_hooks
            end

            it 'executes hook when shutdown' do
              Aws::ActiveJob::SQS.on_worker_stop do
                hook.call
              end
              executor = Executor.new

              executor.shutdown

              expect(hook).to have_received(:call)
            end
          end
        end
      end
    end
  end
end
