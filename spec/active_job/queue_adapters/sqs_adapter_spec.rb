# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    describe SqsAdapter do
      let(:client) { double('Client') }
      before do
        allow(Aws::ActiveJob::SQS.config).to receive(:client).and_return(client)
      end

      it 'enqueues jobs' do
        expect(client).to receive(:send_message)
          .with(
            {
              queue_url: 'https://queue-url',
              message_body: instance_of(String),
              message_attributes: instance_of(Hash)
            }
          )
        TestJob.perform_later('test')
        sleep(0.2)
      end

      context 'fifo queues' do
        before do
          allow(Aws::ActiveJob::SQS.config).to receive(:url_for).and_return('https://queue-url.fifo')
        end

        it 'adds message_deduplication_id and default message_group_id if job does not override it' do
          expect(client).to receive(:send_message)
            .with(
              {
                queue_url: 'https://queue-url.fifo',
                message_body: instance_of(String),
                message_attributes: instance_of(Hash),
                message_group_id: Aws::ActiveJob::SQS.config.message_group_id_for(:default),
                message_deduplication_id: instance_of(String)
              }
            )
          TestJob.perform_later('test')
          sleep(0.2)
        end

        context 'when job has excluded deduplication keys defined' do
          let(:ex_dedup_keys) { %w[job_class queue_name] }
          let(:ex_dudup_keys_with_job_id) { ex_dedup_keys << 'job_id' }
          let(:hashed_body) { 'hashed_body' }

          context 'through #deduplicate_without' do
            before do
              TestJobWithDedupKeys.deduplicate_without(*ex_dedup_keys)
            end

            it 'adds customized message_deduplication_id' do
              expect(Digest::SHA256).to receive(:hexdigest) do |body|
                ex_dudup_keys_with_job_id.each do |key|
                  expect(body).not_to include(%("#{key}"))
                end
              end.and_return(hashed_body)
              expect(client).to receive(:send_message).with(
                {
                  queue_url: 'https://queue-url.fifo',
                  message_body: instance_of(String),
                  message_attributes: instance_of(Hash),
                  message_group_id: Aws::ActiveJob::SQS.config.message_group_id_for(:default),
                  message_deduplication_id: hashed_body
                }
              )

              TestJobWithDedupKeys.perform_later('test')
              sleep(0.2)
            end
          end

          context 'through Aws::ActiveJob::SQS config' do
            before do
              Aws::ActiveJob::SQS.configure do |config|
                config.excluded_deduplication_keys = ex_dedup_keys
              end
            end

            it 'adds customized message_deduplication_id' do
              expect(Digest::SHA256).to receive(:hexdigest) do |body|
                ex_dudup_keys_with_job_id.each do |key|
                  expect(body).not_to include(%("#{key}"))
                end
              end.and_return(hashed_body)
              expect(client).to receive(:send_message).with(
                {
                  queue_url: 'https://queue-url.fifo',
                  message_body: instance_of(String),
                  message_attributes: instance_of(Hash),
                  message_group_id: Aws::ActiveJob::SQS.config.message_group_id_for(:default),
                  message_deduplication_id: hashed_body
                }
              )

              TestJob.perform_later('test')
              sleep(0.2)
            end
          end
        end

        context 'when job has #message_group_id defined' do
          it 'adds message_deduplication_id and default message_group_id if job does not return a value' do
            expect(client).to receive(:send_message).with(
              {
                queue_url: 'https://queue-url.fifo',
                message_body: instance_of(String),
                message_attributes: instance_of(Hash),
                message_group_id: Aws::ActiveJob::SQS.config.message_group_id_for(:default),
                message_deduplication_id: instance_of(String)
              }
            )

            TestJobWithMessageGroupID.perform_later('test')
            sleep(0.2)
          end

          it 'adds message_deduplication_id and given message_group_id if job returns a value' do
            arg = 'test'
            dbl = TestJobWithMessageGroupID.new(arg)
            message_group_id = "mgi_#{rand(0..100)}"

            expect(client).to receive(:send_message).with(
              {
                queue_url: 'https://queue-url.fifo',
                message_body: instance_of(String),
                message_attributes: instance_of(Hash),
                message_group_id: message_group_id,
                message_deduplication_id: instance_of(String)
              }
            )

            expect(TestJobWithMessageGroupID).to receive(:new).with(arg).and_return(dbl)
            expect(dbl).to receive(:message_group_id).and_return(message_group_id)

            TestJobWithMessageGroupID.perform_later(arg)
            sleep(0.2)
          end
        end
      end

      context 'with queue delay' do
        it 'enqueues jobs with proper delay' do
          t1 = Time.now
          allow(Time).to receive(:now).and_return t1

          expect(client).to receive(:send_message).with(
            {
              queue_url: 'https://queue-url',
              delay_seconds: 60,
              message_body: instance_of(String),
              message_attributes: instance_of(Hash)
            }
          )

          TestJob.set(wait: 1.minute).perform_later('test')
          sleep(0.2)
        end

        it 'enqueues jobs with zero or negative delay' do
          t1 = Time.now
          allow(Time).to receive(:now).and_return t1

          expect(client).to receive(:send_message).with(
            {
              queue_url: 'https://queue-url',
              delay_seconds: 0,
              message_body: instance_of(String),
              message_attributes: instance_of(Hash)
            }
          ).twice

          TestJob.set(wait: 0).perform_later('test')
          TestJob.set(wait: -1).perform_later('test')
          sleep(0.2)
        end

        it 'raises an error when job delay is great than SQS support' do
          t1 = Time.now
          allow(Time).to receive(:now).and_return t1
          expect do
            TestJob.set(wait: 1.day).perform_later('test')
          end.to raise_error ArgumentError
        end
      end

      context 'with multiple jobs' do
        before do
          response = double('Response')
          allow(response).to receive(:successful).and_return([1, 2])
          allow(client).to receive(:send_message_batch).and_return(response)
        end

        it do
          expect(client).to receive(:send_message_batch).with(
            {
              queue_url: 'https://queue-url',
              entries: [
                {
                  delay_seconds: instance_of(Integer),
                  id: instance_of(String),
                  message_body: instance_of(String),
                  message_attributes: instance_of(Hash)
                },
                {
                  delay_seconds: instance_of(Integer),
                  id: instance_of(String),
                  message_body: instance_of(String),
                  message_attributes: instance_of(Hash)
                }
              ]
            }
          ).once

          jobs = [
            TestJob.new('test').set(wait: 1.minute),
            TestJob.new('test').set(wait: 1.minute)
          ]
          ActiveJob.perform_all_later(jobs)
        end
      end
    end
  end
end
