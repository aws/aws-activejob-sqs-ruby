# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe JobRunner do
        let(:job_data) { TestJob.new('a1', 'a2').serialize }
        let(:body) { ActiveSupport::JSON.dump(job_data) }
        # message is a reserved minitest name
        let(:msg) { double(data: double(body: body)) }

        it 'parses the job data' do
          job_runner = JobRunner.new(msg)
          expect(job_runner.instance_variable_get(:@job_data)).to eq job_data
        end

        describe '#run' do
          it 'calls Base.execute with the job data' do
            expect(::ActiveJob::Base).to receive(:execute).with(job_data)
            JobRunner.new(msg).run
          end
        end

        describe 'job class validation' do
          def msg_for(class_name)
            double(data: double(body: ActiveSupport::JSON.dump(job_data.merge('job_class' => class_name))))
          end

          it 'rejects classes that do not inherit from ActiveJob::Base' do
            expect { JobRunner.new(msg_for('String')) }
              .to raise_error(InvalidJobClassError, /must inherit from ActiveJob::Base/)
          end

          it 'rejects undefined classes' do
            expect { JobRunner.new(msg_for('NoSuchJob')) }
              .to raise_error(InvalidJobClassError, /is not defined/)
          end

          it 'rejects names that are not well-formed constant paths' do
            ['', 'test_job', 'TestJob; puts 1', 'TestJob()', 'Test Job', '@job', 'Test-Job'].each do |name|
              expect { JobRunner.new(msg_for(name)) }
                .to raise_error(InvalidJobClassError, /is not a valid job class name/)
            end
          end

          it 'rejects a name that resolves only through an ancestor of the namespace' do
            # TestJob does not define ClassMethods, but its ancestors do, so
            # an inheriting lookup would resolve this to
            # ActiveJob::Exceptions::ClassMethods. Constants reachable only
            # through the ancestor chain are not job classes and must not
            # widen what a message can name.
            expect { JobRunner.new(msg_for('TestJob::ClassMethods')) }
              .to raise_error(InvalidJobClassError, /is not defined/)
          end

          it 'does not resolve the constant when the name is malformed' do
            expect_any_instance_of(JobRunner).not_to receive(:constantize_job_class)
            expect { JobRunner.new(msg_for('not_a_class_name')) }.to raise_error(InvalidJobClassError)
          end

          context 'with job_class_allowlist configured' do
            before { Aws::ActiveJob::SQS.config.job_class_allowlist = [TestJob] }
            after { Aws::ActiveJob::SQS.config.job_class_allowlist = nil }

            it 'allows classes in the allowlist' do
              expect { JobRunner.new(msg) }.not_to raise_error
            end

            it 'rejects classes not in the allowlist' do
              expect { JobRunner.new(msg_for('TestJobAsync')) }
                .to raise_error(InvalidJobClassError, /not in the configured job_class_allowlist/)
            end

            it 'rejects a disallowed name without resolving its constant' do
              # Resolving is what triggers autoloading, so a name the allowlist
              # excludes must be rejected before the lookup happens.
              expect_any_instance_of(JobRunner).not_to receive(:constantize_job_class)
              expect { JobRunner.new(msg_for('TestJobAsync')) }
                .to raise_error(InvalidJobClassError, /not in the configured job_class_allowlist/)
            end

            it 'rejects a disallowed name even when it is undefined' do
              expect { JobRunner.new(msg_for('NoSuchJob')) }
                .to raise_error(InvalidJobClassError, /not in the configured job_class_allowlist/)
            end
          end

          context 'with a string array allowlist (as loaded from YAML)' do
            before { Aws::ActiveJob::SQS.config.job_class_allowlist = ['TestJob'] }
            after { Aws::ActiveJob::SQS.config.job_class_allowlist = nil }

            it 'allows classes whose name is in the allowlist' do
              expect { JobRunner.new(msg) }.not_to raise_error
            end

            it 'rejects classes whose name is not in the allowlist' do
              expect { JobRunner.new(msg_for('TestJobAsync')) }
                .to raise_error(InvalidJobClassError, /not in the configured job_class_allowlist/)
            end
          end

          context 'with a comma-separated string allowlist (as loaded from ENV)' do
            before { Aws::ActiveJob::SQS.config.job_class_allowlist = 'SomeJob, TestJob ,OtherJob' }
            after { Aws::ActiveJob::SQS.config.job_class_allowlist = nil }

            it 'allows classes whose name is in the allowlist, ignoring whitespace' do
              expect { JobRunner.new(msg) }.not_to raise_error
            end

            it 'rejects classes whose name is not in the allowlist' do
              expect { JobRunner.new(msg_for('TestJobAsync')) }
                .to raise_error(InvalidJobClassError, /not in the configured job_class_allowlist/)
            end
          end
        end
      end
    end
  end
end
