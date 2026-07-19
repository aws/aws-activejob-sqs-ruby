# frozen_string_literal: true

describe 'ActiveJob::Base#sqs_message' do
  it 'defines sqs_message accessor on ActiveJob::Base' do
    job = TestJob.new('a1', 'a2')
    expect(job).to respond_to(:sqs_message)
    expect(job).to respond_to(:sqs_message=)
  end

  it 'defaults to nil' do
    job = TestJob.new('a1', 'a2')
    expect(job.sqs_message).to be_nil
  end

  it 'can be set and read' do
    job = TestJob.new('a1', 'a2')
    fake_message = double('sqs_message')
    job.sqs_message = fake_message
    expect(job.sqs_message).to eq(fake_message)
  end
end
