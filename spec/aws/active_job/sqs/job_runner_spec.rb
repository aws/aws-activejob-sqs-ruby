# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe JobRunner do
        let(:job_serialized) { TestJob.new('a1', 'a2').serialize }
        let(:body) { ActiveSupport::JSON.dump(job_serialized) }
        let(:message_id) { '12345' }
        let(:job_data) { job_serialized.merge('provider_job_id' => message_id) }
        let(:msg) do
          instance_double(Aws::SQS::Message, data: double(body: body, message_id: message_id))
        end
        let(:instance) { described_class.new(msg) }

        it 'parses the job data' do
          expect(instance.instance_variable_get(:@job_data)).to eq(job_data)
        end

        describe '#run' do
          subject { instance.run }

          it 'calls Base.execute with the job data' do
            expect(::ActiveJob::Base).to receive(:execute).with(job_data).and_call_original
            subject
          end
        end
      end
    end
  end
end
