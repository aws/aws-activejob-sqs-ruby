# frozen_string_literal: true

module Aws
  module ActiveJob
    module SQS
      describe Configuration do
        let(:expected_file_opts) do
          {
            max_messages: 5,
            queues: { default: { url: 'https://queue-url', max_messages: 2 } }
          }
        end

        it 'configures defaults without runtime or YAML options' do
          allow(File).to receive(:exist?).and_return(false)
          cfg = Aws::ActiveJob::SQS::Configuration.new
          expect(cfg.to_h).to include(Aws::ActiveJob::SQS::Configuration::DEFAULTS)
        end

        it 'merges runtime options with default options' do
          allow(File).to receive(:exist?).and_return(false)
          cfg = Aws::ActiveJob::SQS::Configuration.new(shutdown_timeout: 360)
          expect(cfg.shutdown_timeout).to eq 360
        end

        it 'merges YAML options with default options' do
          cfg = Aws::ActiveJob::SQS::Configuration.new
          expected = Aws::ActiveJob::SQS::Configuration::DEFAULTS.merge(expected_file_opts)
          expect(cfg.to_h).to include(expected)
        end

        it 'merges runtime options with YAML options' do
          cfg = Aws::ActiveJob::SQS::Configuration.new(shutdown_timeout: 360)
          expected = Aws::ActiveJob::SQS::Configuration::DEFAULTS
                     .merge(expected_file_opts)
                     .merge(shutdown_timeout: 360)
          expect(cfg.to_h).to include(expected)
        end

        # For Ruby 3.1+, Psych 4 will normally raise BadAlias error
        it 'accepts YAML config with alias' do
          allow_any_instance_of(ERB).to receive(:result).and_return(<<~YAML)
            common: &common
              default:
                url: 'https://queue-url'
            queues:
              <<: *common
          YAML
          expect { Aws::ActiveJob::SQS::Configuration.new }.to_not raise_error
        end

        context 'ENV set' do
          Configuration::GLOBAL_ENV_CONFIGS.each do |config_name|
            next if config_name == :config_file

            describe "ENV #{config_name}" do
              let(:env_name) { "AWS_ACTIVE_JOB_SQS_#{config_name.to_s.upcase}" }

              let(:cfg) { Configuration.new }

              before(:each) do
                ENV[env_name] = 'env_value'

                file_options = {}
                file_options[config_name] = 'file_value'
                allow_any_instance_of(Configuration)
                  .to receive(:file_options).and_return(file_options)
              end

              after(:each) do
                ENV.delete(env_name)
              end

              it 'uses values from ENV over default and file' do
                expect(cfg.send(config_name)).to eq('env_value')
              end

              it 'uses runtime configured values over ENV' do
                options = {}
                options[config_name] = 'runtime_value'
                cfg = Configuration.new(options)
                expect(cfg.send(config_name)).to eq('runtime_value')
              end
            end
          end

          Configuration::QUEUE_ENV_CONFIGS.each do |config_name|
            describe "ENV queue #{config_name}" do
              let(:env_name) { "AWS_ACTIVE_JOB_SQS_DEFAULT_#{config_name.to_s.upcase}" }

              let(:cfg) { Configuration.new }

              before(:each) do
                ENV[env_name] = 'env_value'

                file_options = { queues: { default: {} } }
                file_options[:queues][:default][config_name] = 'file_value'
                allow_any_instance_of(Configuration)
                  .to receive(:file_options).and_return(file_options)
              end

              after(:each) do
                ENV.delete(env_name)
              end

              it 'uses values from ENV over default and file' do
                puts cfg.to_h
                expect(cfg.send(:"#{config_name}_for", :default)).to eq('env_value')
              end

              it 'uses runtime configured values over ENV' do
                options = { queues: { default: {} } }
                options[:queues][:default][config_name] = 'runtime_value'
                cfg = Configuration.new(options)
                expect(cfg.send(:"#{config_name}_for", :default)).to eq('runtime_value')
              end
            end
          end
        end

        describe '#client' do
          it 'does not create client on initialize' do
            expect(Aws::SQS::Client).not_to receive(:new)
            Aws::ActiveJob::SQS::Configuration.new
          end

          it 'creates a client on #client' do
            client = Aws::SQS::Client.new(stub_responses: true)
            cfg = Aws::ActiveJob::SQS::Configuration.new
            expect(Aws::SQS::Client).to receive(:new).and_return(client)
            cfg.client
          end
        end

        Configuration::QUEUE_ENV_CONFIGS.each do |config_name|
          describe "##{config_name}_for" do
            let(:cfg) do
              queues = {
                default: {},
                override: {}
              }
              queues[:override][config_name] = 'queue_value'
              options = { queues: queues, config_file: 'nonexistant' }
              options[config_name] = 'global_value'
              Aws::ActiveJob::SQS::Configuration.new(**options)
            end

            it 'returns the queue value when set' do
              expect(cfg.send(:"#{config_name}_for", :override)).to eq('queue_value')
            end

            it 'returns the global value when unset' do
              expect(cfg.send(:"#{config_name}_for", :default)).to eq('global_value')
            end

            it 'raises an ArgumentError when the queue is not mapped' do
              expect { cfg.send(:"#{config_name}_for", :not_mapped) }.to raise_error(ArgumentError)
            end
          end
        end
      end
    end
  end
end
