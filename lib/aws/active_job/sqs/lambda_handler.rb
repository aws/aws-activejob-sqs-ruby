# frozen_string_literal: true

require 'aws-sdk-sqs'

module Aws
  module ActiveJob
    module SQS
      # Lambda event handler to run jobs from an SQS queue trigger
      module LambdaHandler
        class << self
          # A lambda event handler to run jobs from an SQS queue trigger.
          # Configure the entrypoint to: +config/environment.Aws::ActiveJob::SQS::LambdaHandler.job_handler+
          # This will load your Rails environment, and then use this method as the handler.
          def job_handler(event:, context:)
            return 'no records to process' unless event['Records']

            puts "job_handler running for #{event} with context: #{context}"

            event['Records'].each do |record|
              sqs_msg = to_sqs_msg(record)
              job = Aws::ActiveJob::SQS::JobRunner.new(sqs_msg)
              puts "Running job: #{job.id}[#{job.class_name}]"
              job.run
              sqs_msg.delete
            end
            "Processed #{event['Records'].length} jobs."
          end

          private

          def to_sqs_msg(record)
            data = {
              body: record['body'],
              md5_of_body: record['md5OfBody'],
              message_attributes: to_message_attributes(record),
              message_id: record['messageId'],
              receipt_handle: record['receiptHandle']
            }
            Aws::SQS::Message.new(
              queue_url: to_queue_url(record),
              receipt_handle: msg[:receipt_handle],
              data: data,
              client: Aws::ActiveJob::SQS.config.client
            )
          end

          def to_message_attributes(record)
            record['messageAttributes'].transform_values do |value|
              {
                string_value: value['stringValue'],
                binary_value: value['binaryValue'],
                string_list_values: ['stringListValues'],
                binary_list_values: value['binaryListValues'],
                data_type: value['dataType']
              }
            end
          end

          def to_queue_url(record)
            source_arn = record['eventSourceARN']
            raise ArgumentError, "Invalid queue arn: #{source_arn}" unless Aws::ARNParser.arn?(source_arn)

            arn = Aws::ARNParser.parse(source_arn)
            sfx = Aws::Partitions::EndpointProvider.dns_suffix_for(arn.region)
            "https://sqs.#{arn.region}.#{sfx}/#{arn.account_id}/#{arn.resource}"
          end
        end
      end
    end
  end
end
