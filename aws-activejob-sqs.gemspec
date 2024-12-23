# frozen_string_literal: true

version = File.read(File.expand_path('VERSION', __dir__)).strip

Gem::Specification.new do |spec|
  spec.name         = 'aws-activejob-sqs'
  spec.version      = version
  spec.author       = 'Amazon Web Services'
  spec.email        = ['aws-dr-rubygems@amazon.com']
  spec.summary      = 'ActiveJob integration with SQS'
  spec.description  = 'Amazon Simple Queue Service as an ActiveJob adapter'
  spec.homepage     = 'https://github.com/aws/aws-activejob-sqs-ruby'
  spec.license      = 'Apache-2.0'
  spec.files        = Dir['LICENSE', 'CHANGELOG.md', 'VERSION', 'lib/**/*']
  spec.executables  = ['aws_sqs_active_job', 'aws_active_job_sqs']

  # Require this version for user_agent_framework configs
  spec.add_dependency('aws-sdk-sqs', '~> 1', '>= 1.56.0')

  spec.add_dependency('activejob', '>= 7.1.0')
  spec.add_dependency('concurrent-ruby', '~> 1')

  spec.required_ruby_version = '>= 2.7'
end
