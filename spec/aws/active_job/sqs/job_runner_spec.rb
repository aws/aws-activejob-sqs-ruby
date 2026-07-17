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
          it 'rejects classes that do not inherit from ActiveJob::Base' do
            bad_data = job_data.merge('job_class' => 'String')
            bad_body = ActiveSupport::JSON.dump(bad_data)
            bad_msg = double(data: double(body: bad_body))
            expect { JobRunner.new(bad_msg) }.to raise_error(ArgumentError, /not a valid job class/)
          end

          context 'with job_class_allowlist configured' do
            before { Aws::ActiveJob::SQS.config.job_class_allowlist = [TestJob] }
            after { Aws::ActiveJob::SQS.config.job_class_allowlist = nil }

            it 'allows classes in the allowlist' do
              expect { JobRunner.new(msg) }.not_to raise_error
            end

            it 'rejects classes not in the allowlist' do
              other_data = job_data.merge('job_class' => 'TestJobAsync')
              other_body = ActiveSupport::JSON.dump(other_data)
              other_msg = double(data: double(body: other_body))
              expect { JobRunner.new(other_msg) }.to raise_error(ArgumentError, /not in the configured job_class_allowlist/)
            end
          end
        end
      end
    end
  end
end
