# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe Executor do
        let(:logger) { double(info: nil, debug: nil) }

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
              executor.shutdown # give the job a chance to run
            end.to raise_exception(StandardError)
          end

          describe 'error_handler' do
            let(:error_handler) { double }
            let(:executor) { Executor.new(error_handler: error_handler) }
            let(:exception) { StandardError.new }

            it 'calls the error handler with exception and message' do
              expect(JobRunner).to receive(:new).and_return(runner)
              expect(runner).to receive(:run).and_raise exception
              expect(error_handler).to receive(:call).with(exception, msg)
              expect(executor).to receive(:shutdown).exactly(1).times.and_call_original

              executor.execute(msg)
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
