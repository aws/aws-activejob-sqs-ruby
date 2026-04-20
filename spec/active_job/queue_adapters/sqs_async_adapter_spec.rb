# frozen_string_literal: true

module ActiveJob
  module QueueAdapters
    describe SqsAsyncAdapter do
      let(:client) { double('Client') }
      let(:message_result) { instance_double(Aws::SQS::Types::SendMessageResult, message_id: '12345') }

      before do
        allow(Aws::ActiveJob::SQS.config).to receive(:client).and_return(client)
      end

      def mock_send_message
        expect(client).to receive(:send_message).with(
          {
            queue_url: 'https://queue-url',
            message_attributes: instance_of(Hash),
            message_body: include("\"locale\":\"#{I18n.locale}\"")
          }
        )
      end

      def mock_async
        expect(Concurrent::Promises).to receive(:future).and_call_original
      end

      it 'enqueues jobs without blocking' do
        mock_send_message
        mock_async

        TestJobAsync.perform_later('test')
        sleep(0.2)
      end

      it 'calls the custom error handler when set' do
        expect(client).to receive(:send_message).and_raise('error')
        allow(Aws::ActiveJob::SQS.config)
          .to receive(:async_queue_error_handler)
          .and_return(proc { @error_handled = true })

        TestJobAsync.perform_later('test')
        sleep(0.2)

        expect(@error_handled).to be true
      end

      it 'passes the serialized I18n locale to promises' do
        I18n.available_locales = %i[en de] # necessary, defaults empty

        I18n.with_locale(:de) do
          mock_async
          mock_send_message

          TestJobAsync.perform_later('test')
          sleep(0.2)
        end

        I18n.available_locales = []
      end

      it 'queues jobs to fifo queues synchronously' do
        allow(Aws::ActiveJob::SQS.config).to receive(:url_for)
          .and_return('https://queue-url.fifo')
        expect(Concurrent::Promises).not_to receive(:future)
        expect(client).to receive(:send_message).and_return(message_result)

        TestJobAsync.perform_later('test')
        sleep(0.2)
      end
    end
  end
end
