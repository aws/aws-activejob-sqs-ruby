# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe JobRunner do
        subject { described_class.new(msg) }

        let(:job_data) { TestJob.new('a1', 'a2').serialize }
        let(:event_body) { 'example sqs message' }
        let(:body) { ActiveSupport::JSON.dump(job_data) }
        let(:message_id) { SecureRandom.uuid }
        let(:active_job_attributes) do
          {
            'aws_sqs_active_job_class' => { 
              'string_value' => 'TestJob',
              'data_type' => 'String'
            },
            'aws_sqs_active_job_version' => {
              'string_value' => '1.0.0',
              'data_type' => 'String'
            }
          }
        end
        # message is a reserved minitest name
        let(:msg) do
          double( data: double(body: body),
                  message_id: message_id,
                  queue_url: queue_config.dig(:default_queue, :url),
                  message_attributes: active_job_attributes)
        end
        let(:event_msg) do
          double( data: double(message_id: SecureRandom.uuid, body: event_body, message_attributes: {}),
                  queue_url: queue_config.dig(:event_queue, :url),
                  message_id: message_id,
                  message_attributes: {})
        end
        let(:queue_config) do
          {
            default_queue: {
              url: 'http://example.sqs/default_queue',
            },
            event_queue: {
              url: 'http://example.sqs/event_queue',
              job_class: 'EventJob'
            }
          }
        end

        before do
          described_class.class_variable_set(:@@queue_handlers, nil)
          allow(Aws::ActiveJob::SQS.config).to receive(:queues).and_return(queue_config).once
        end

        it 'parses the job data' do
          job_runner = JobRunner.new(msg)
          expect(job_runner.instance_variable_get(:@job_data)).to eq job_data
        end

        describe '.queue_event_handlers' do
          context 'has no event queues' do
            it 'returns empty hash' do
              allow(Aws::ActiveJob::SQS.config).to receive(:queues).and_return(queue_config.except(:event_queue))
              expect(described_class.queue_event_handlers).to eq({})
            end
          end

          context 'has event queues' do
            it 'returns a hash of queue urls to job classes' do
              expect(described_class.queue_event_handlers).to eq(
                'http://example.sqs/event_queue' => 'EventJob'
              )
            end
          end
        end

        describe '#run' do
          it 'calls Base.execute with the job data' do
            expect(::ActiveJob::Base).to receive(:execute).with(job_data)
            JobRunner.new(msg).run
          end
        end

        describe '#prepare_job_data' do
          before { subject } # initialize the subject

          context 'active job message' do
            it 'returns the job data' do
              expect(ActiveSupport::JSON).to receive(:load).with(body).and_call_original
              expect(subject.send(:prepare_job_data, msg)).to eq job_data
            end
          end

          context 'event message' do
            it 'invokes format_event_data' do
              expect(subject).to receive(:format_event_data).with(event_msg).and_call_original
              subject.send(:prepare_job_data, event_msg)
            end
          end
        end

        describe '#format_event_data' do
          let(:event_job) { described_class.new(event_msg) }

          it 'returns a hash with job_class, job_id, and arguments' do
            expect(event_job.send(:format_event_data, event_msg)).to eq(
              'job_class' => 'EventJob',
              'job_id' => event_msg.message_id,
              'arguments' => [event_msg.data.to_json]
            )
          end
        end

        describe '#is_active_job_message?' do
          it 'returns true if the message has active job attributes' do
            expect(subject.send(:is_active_job_message?, msg)).to be true
          end

          it 'returns false if the message does not have active job attributes' do
            expect(subject.send(:is_active_job_message?, event_msg)).to be false
          end
        end
      end
    end
  end
end
