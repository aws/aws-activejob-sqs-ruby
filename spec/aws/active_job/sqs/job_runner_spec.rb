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

        it 'stores the raw SQS message' do
          job_runner = JobRunner.new(msg)
          expect(job_runner.instance_variable_get(:@message)).to eq msg
        end

        describe '#run' do
          it 'deserializes the job and calls perform_now' do
            job_instance = instance_double(TestJob)
            allow(::ActiveJob::Base).to receive(:deserialize).with(job_data).and_return(job_instance)
            allow(job_instance).to receive(:sqs_message=)
            allow(job_instance).to receive(:perform_now)

            JobRunner.new(msg).run

            expect(job_instance).to have_received(:perform_now)
          end

          it 'sets sqs_message on the job instance' do
            job_instance = instance_double(TestJob)
            allow(::ActiveJob::Base).to receive(:deserialize).with(job_data).and_return(job_instance)
            allow(job_instance).to receive(:sqs_message=)
            allow(job_instance).to receive(:perform_now)

            JobRunner.new(msg).run

            expect(job_instance).to have_received(:sqs_message=).with(msg)
          end

          it 'skips setting sqs_message if the job does not respond to it' do
            job_instance = instance_double(TestJob)
            allow(::ActiveJob::Base).to receive(:deserialize).with(job_data).and_return(job_instance)
            allow(job_instance).to receive(:respond_to?).with(:sqs_message=).and_return(false)
            allow(job_instance).to receive(:perform_now)

            expect { JobRunner.new(msg).run }.not_to raise_error
            expect(job_instance).to have_received(:perform_now)
          end
        end
      end
    end
  end
end
