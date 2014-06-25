require 'sqskiq/signal_handler'

module Sqskiq
  class Processor
    include Celluloid
    include Sqskiq::SignalHandler

    def initialize(worker_class, exception_handler)
      @worker_instance = worker_class.new
      @exception_handler = exception_handler
      subscribe_for_shutdown
    end

    def process(message)
      return  { :success => false, :message => message } if @shutting_down

      result = true
      begin
        @worker_instance.perform(message)
      rescue Exception => e
        result = false
        if !@exception_handler.nil? && @worker_instance.respond_to?(@exception_handler)
          @worker_instance.send(@exception_handler, e)
        end
      ensure
        ::ActiveRecord::Base.clear_active_connections! if defined?(::ActiveRecord)
      end
      return { :success => result, :message => message }
    end

  end
end
