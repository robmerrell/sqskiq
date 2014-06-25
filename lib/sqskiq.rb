require 'sqskiq/worker'

module Sqskiq
  def self.initialize!
    require 'celluloid'
    require 'celluloid/autostart'

    require "sqskiq/manager"
    require 'sqskiq/fetch'
    require 'sqskiq/process'
    require 'sqskiq/delete'
    require 'sqskiq/batch_process'
  end

  # Configures and starts actor system
  def self.bootstrap(worker_config, worker_class)
    initialize!

    config = valid_config_from(worker_config)
    credentials = [config[:queue_name], @configuration]

    Celluloid::Actor[:manager]   = @manager   = Manager.new(config[:empty_queue_throttle])
    Celluloid::Actor[:fetcher]   = @fetcher   = Fetcher.pool(:size => config[:num_fetchers], :args => credentials)
    Celluloid::Actor[:deleter]   = @deleter   = Deleter.pool(:size => config[:num_deleters], :args => credentials)
    Celluloid::Actor[:processor] = @processor = Processor.pool(:size => config[:num_workers], :args => [worker_class, config[:on_exception]])
    Celluloid::Actor[:batcher]   = @batcher   = BatchProcessor.pool(:size => config[:num_batches])

    run!
  rescue Interrupt
    exit 0
  end

  # Subscribes actors to receive system signals
  # Each actor when receives a signal should execute
  # appropriate code to exit cleanly
  def self.run!
    self_read, self_write = IO.pipe

    ['SIGTERM', 'TERM', 'SIGINT'].each do |sig|
      begin
        trap sig do
          self_write.puts(sig)
        end
      rescue ArgumentError
        puts "Signal #{sig} not supported"
      end
    end

    begin
      @manager.bootstrap

      while readable_io = IO.select([self_read])
        signal = readable_io.first[0].gets.strip

        @manager.publish('SIGTERM')
        @batcher.publish('SIGTERM')
        @processor.publish('SIGTERM')

        while @manager.running?
          sleep 2
        end

        @manager.terminate

        break
      end
    end
  end

  ##
  # checks the provided configuration
  # and add the defaults when not specified
  def self.valid_config_from(worker_config)
    num_workers = (worker_config[:processors].nil? || worker_config[:processors].to_i < 2)? 20 : worker_config[:processors]
    # messy code due to celluloid pool constraint of 2 as min pool size: see spec for better understanding
    num_fetchers = num_workers / 10
    num_fetchers = num_fetchers + 1 if num_workers % 10 > 0
    num_fetchers = 2 if num_fetchers < 2
    num_deleters = num_batches = num_fetchers

    {
      num_workers: num_workers,
      num_fetchers: num_fetchers,
      num_batches: num_batches,
      num_deleters: num_deleters,
      queue_name: worker_config[:queue_name],
      empty_queue_throttle: worker_config[:empty_queue_throttle] || 0,
      on_exception: worker_config[:on_exception]
    }
  end

  def self.configure
    yield self
  end

  def self.configuration
    @configuration
  end

  def self.configuration=(value)
    @configuration = value
  end
end
