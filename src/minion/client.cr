require "./client/version"
require "socket"
require "msgpack"
require "retriable"

module Minion

  class Client
    class FailedToAuthenticate < Exception
      def initialize(destination = "UNK", port = 6766)
        super("Failed to authenticate to the Minion server at #{destination}:#{port}")
      end
    end

    MAX_MESSAGE_LENGTH = 8192
    MAX_LENGTH_BYTES = MAX_MESSAGE_LENGTH.to_s.length
    CONNECTION_FAILURE_TIMEOUT = 86_400 * 2 # Log locally for a long time if Minion server goes down.
    MAX_FAILURE_COUNT = 0_u128 &- 1 # Max integer -- i.e. really big
    PERSISTENT_QUEUE_LIMIT = 10_737_412_742 # Default to allowing around 10GB temporary local log storage
    RECONNECT_THROTTLE_INTERVAL = 0.1

    def log(severity, msg)
      if @destination == :local
        _local_log(@service, severity, msg)
      else
        _remote_log(@service, severity, msg)
      end
    rescue Exception
      @authenticated = false
      setup_local_logging
      setup_reconnect_fiber
    end

    #----- Various class accessors -- use these to set defaults

    @@connection_failure_timeout : Int32 = CONNECTION_FAILURE_TIMEOUT
    def self.connection_failure_timeout
      @@connection_failure_timeout
    end

    def self.connection_failure_timeout=(val)
      @@connection_failure_timeout = val.to_i
    end

    @@max_failure_count : UInt128 = MAX_FAILURE_COUNT
    def self.max_failure_count
      @@max_failure_count
    end

    def self.max_failure_count=(val)
      @@max_failure_count = val.to_i
    end

    @@persistent_queue_limit = PERSISTENT_QUEUE_LIMIT
    def self.persistent_queue_limit
      @@persistent_queue_limit ||= PERSISTENT_QUEUE_LIMIT
    end

    def self.persistent_queue_limit=(val)
      @@persistent_queue_limit = val.to_i
    end

    def self.reconnect_throttle_interval
      @@reconnect_throttle_interval ||= RECONNECT_THROTTLE_INTERVAL
    end

    def self.reconnect_throttle_interval=(val)
      @@reconnect_throttle_interval = val.to_i
    end

    #-----

    @socket : TCPSocket?
    @swamp_drainer : Fiber?
    @reconnection_thread : Fiber?
    @failed_at : Time?
    @connection_failure_timeout : Int32
    @max_failure_count : UInt128
    @persistent_queue_limit : Int64
    @message_buffer : Slice(UInt8)
    @tmplog : String?
    @reconnect_throttle_interval : Float64
    def initialize(@service = "default", @host = "127.0.0.1", @port = 6766, @key = "")
      @socket = nil
      klass = self.class
      @connection_failure_timeout = klass.connection_failure_timeout
      @max_failure_count = klass.max_failure_count
      @persistent_queue_limit = klass.persistent_queue_limit
      @reconnect_throttle_interval = klass.reconnect_throttle_interval
      @destination = :remote
      @reconnection_thread = nil
      @authenticated = false
      @total_count = 0
      @logfile = nil
      @swamp_drainer = nil
      @failed_at = nil
      @send_size_buffer = Slice(UInt8).new(2)
      @receive_size_buffer = Slice(UInt8).new(2)
      @size_read = 0
      @message_bytes_read = 0
      @message_size = 0_u16
      @data_buffer = Slice(UInt8).new(MAX_MESSAGE_LENGTH)
      @message_buffer = @data_buffer[0,1]
      @read_message_size = true
      @read_message_body = false

      clear_failure

      connect
    end

    #----- Various instance accessors

    getter total_count

    getter connection_failure_timeout

    def connection_failure_timeout=(val)
      @connection_failure_timeout = val.to_i
    end

    getter max_failure_count

    def max_failure_count=(val)
      @max_failure_count = val.to_i
    end

    getter ram_queue_limit

    def ram_queue_limit=(val)
      @ram_queue_limit = val.to_i
    end

    getter persistent_queue_limit

    def persistent_queue_limit=(val)
      @persistent_queue_limit = val.to_i
    end

    def tmplog_prefix
      File.join(Dir.tempdir, "minion-SERVICE-PID.log")
    end

    def tmplog
      @tmplog ||= tmplog_prefix.gsub(/SERVICE/, @service).gsub(/PID/, Process.pid.to_s)
    end

    def tmplogs
      Dir[tmplog_prefix.gsub(/SERVICE/, @service).gsub(/PID/, "*")].sort_by { |f| File.info(f).modification_time }
    end

    setter tmplog

    def reconnect_throttle_interval
      @reconnect_throttle_interval ||= self.class.reconnect_throttle_interval
    end

    def reconnect_throttle_interval=(val)
      @reconnect_throttle_interval = val.to_i
    end

    #----- The meat of the client

    def connect
      @socket = open_connection(@host, @port)

      authenticate
      raise FailedToAuthenticate.new(@host, @port) unless authenticated?

      clear_failure

      if there_is_a_swamp?
        drain_the_swamp
      else
        setup_remote_logging
      end
    rescue e : Exception
      STDERR.puts e
      STDERR.puts e.backtrace.inspect
      register_failure
      close_connection
      setup_reconnect_fiber unless @reconnection_thread && !@reconnection_thread.not_nil!.dead?
      setup_local_logging
      raise e if fail_connect?
    end

    # Read a message from the wire using a length header before the msgpack payload.
    def read
      if @read_message_size
        if @size_read == 0
          @size_read = @socket.not_nil!.read(@send_size_buffer)
          if @size_read < 2 
            Fiber.yield
          end
        elsif @size_read == 1
         byte = @socket.not_nil!.read_byte
         if byte
           @send_size_buffer[1] = byte
           @size_read = 2
         end
       end

       if @size_read > 1
        @read_message_body = true
        @read_message_size = false
        @size_read = 0
       end
      end

      if @read_message_body
        if @message_size == 0
          @message_size = IO::ByteFormat::BigEndian.decode(UInt16, @send_size_buffer)
          @message_buffer = @data_buffer[0, @message_size]
        end
        if @message_bytes_read < @message_size
          # Try to read the rest of the bytes.
          remaining_bytes = @message_size - @message_bytes_read
          read_buffer = @message_buffer[@message_bytes_read, remaining_bytes]
          bytes_read = @socket.not_nil!.read(read_buffer)
          @message_bytes_read += bytes_read
        end

        if @message_bytes_read >= @message_size
          #msg = Tuple(String, String, String).from_msgpack(@message_buffer) TODO: Handle different types right.
          msg = String.from_msgpack(@message_buffer)
          @read_message_body = false
          @read_message_size = true
          @message_size = 0
          @message_bytes_read = 0

          return msg
        else
          Fiber.yield
        end
      end
    end

    def setup_local_logging
      return if @logfile && !@logfile.not_nil!.closed?

      @logfile = File.open(tmplog, "a+")
      @destination = :local
    end

    def setup_remote_logging
      @destination = :remote
    end

    def setup_reconnect_fiber
      return if @reconnection_thread

      @reconnection_thread = spawn do
        loop do
          sleep reconnect_throttle_interval
          begin
            connect
          rescue Exception
            nil
          end
          break if @socket && !closed?
        end
        @reconnection_thread = nil
      end
    end

    def _remote_log(service, severity, message, flush_after_send = true)
      # @total_count += 1
      # len = MAX_LENGTH_BYTES + MAX_LENGTH_BYTES + service.length + severity.length + message.length + 3
      # ll = format("%0#{MAX_LENGTH_BYTES}i%0#{MAX_LENGTH_BYTES}i", len, len)
      # @socket.write "#{ll}:#{service}:#{severity}:#{message}"
      msg = [service, severity, message]
      packed_msg = msg.to_msgpack
      IO::ByteFormat::BigEndian.encode(packed_msg.size.to_u16, @send_size_buffer)
      @socket.not_nil!.write(@send_size_buffer)
      @socket.not_nil!.write(packed_msg)
      @socket.not_nil!.flush if flush_after_send
    end

    def _local_log(service, severity, message)
      # Convert newlines to a different marker so that log messages can be stuffed onto a single file line.
      @logfile.not_nil!.flock_exclusive
      @logfile.not_nil!.write "#{service}:#{severity}:#{message.gsub(/\n/, "\x00\x00")}\n".to_slice
    ensure
      @logfile.not_nil!.flock_unlock
    end

    def open_connection(host, port)
      TCPSocket.new(host, port)
    end

    def close_connection
      @socket.not_nil!.close if @socket && !@socket.not_nil!.closed?
    end

    def register_failure
      @failed_at ||= Time.local
      @failure_count = @failure_count.not_nil! + 1
    end

    def fail_connect?
      failed_too_many? || failed_too_long?
    end

    def failed?
      !@failed_at.nil?
    end

    def failed_too_many?
      @failure_count.not_nil! > @max_failure_count
    end

    def failed_too_long?
      failed? && (@failed_at.not_nil! + Time::Span.new(seconds: @connection_failure_timeout)) < Time.local
    end

    def clear_failure
      @failed_at = nil
      @failure_count = 0
    end

    def authenticate
      begin
        _remote_log(@service, "authentication", @key.to_s, true)
        response = read
      rescue e : Exception
        STDERR.puts "\nauthenticate: #{e}\n#{e.backtrace.join("\n")}"
        response = nil
      end

      @authenticated = if response && response =~ /accepted/
                         true
                       else
                         false
                       end
    end

    def there_is_a_swamp?
      tmplogs.each do |logfile|
        break true if File.exists?(logfile) && File.size(logfile) > 0
      end
    end

    def drain_the_swamp
      @swamp_drainer = spawn _drain_the_swamp unless @swamp_drainer && !@swamp_drainer.not_nil!.dead?
    end

    def non_blocking_lock_on_file_handle(file_handle)
      file_handle.flock_exclusive(false) ? yield : false
    ensure
      file_handle.flock_unlock
    end

    def _drain_the_swamp
      # As soon as we start emptying the local log file, ensure that no data
      # gets missed because of IO buffering. Otherwise, during high rates of
      # message sending, it is possible to get an EOF on file reading, and
      # assume all data has been sent, when there are actually records which
      # are buffered and just haven't been written yet.
      @logfile && (@logfile.not_nil!.sync = true)

      tmplogs.each do |logfile|
        read_buffer = Slice(UInt8).new(8192)

        File.exists?(logfile) && File.open(logfile) do |fh|
          non_blocking_lock_on_file_handle(fh) do # Only one process should read a given file.
            fh.fsync
            logfile_not_empty = true
            while logfile_not_empty
              record = fh.gets unless closed?
              if record
                next if record =~ /^\#/

                service, severity, msg = record.split(":", 3)
                msg = msg.chomp.gsub(/\x00\x00/, "\n")
                Retriable.retry(max_interval: 1.minute, max_attempts: 0_u32 &- 1, multiplier: 1.05) do
                  _remote_log(service, severity, msg)
                end
              else
                logfile_not_empty = false
              end
            end
            File.delete logfile
          end
          setup_remote_logging if tmplog == logfile
        end
      end

      @swamp_drainer = nil
    rescue e : Exception
      STDERR.puts "ERROR SENDING LOCALLY SAVED LOGS: #{e}\n#{e.backtrace.inspect}"
    end

    def authenticated?
      @authenticated
    end

    def reconnect
      connect(@host, @port)
    end

    def close
      @socket.not_nil!.close
    end

    def closed?
      @socket.not_nil!.closed?
    end
  end
end
