# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe Deduplication do
        describe 'ClassMethods' do
          describe '.deduplicate_without' do
            let(:keys) { %w[job_id job_class queue_name] }
            let(:expected_keys) { keys.map(&:to_s) | ['job_id'] }

            it 'excluded deduplication keys set successfully' do
              expect(TestJobWithDedupKeys.deduplicate_without(*keys)).to contain_exactly(*expected_keys)
            end

            it 'excluded deduplication keys set successfully and job_id is added' do
              keys.delete(:job_id)
              expect(TestJobWithDedupKeys.deduplicate_without(*keys)).to contain_exactly(*expected_keys)
            end
          end
        end
      end
    end
  end
end
