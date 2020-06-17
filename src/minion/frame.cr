require "./uuid"
require "msgpack"

module Minion
  #
  # Represents a chunk of data to be transmitted, or that has been received.
  #
  # Verbs:
  # L -- log
  # C - Command
  # R - Response
  # T - Telemetry
  # Q - Query Key/Value Store
  # S - Set Key/Value
  struct Frame
    def self.from_msgpack(buffer : Slice(UInt8))
      unpacked = Tuple(String, String, Array(String)).from_msgpack(buffer).as(Tuple(String, String, Array(String)))
    end

    def self.inflate(tup : Tuple(String, String, Array(String)))
      {UUID.new(tup[1].to_s, tup[1], tup[2])}
    end

    property verb
    property uuid
    property data

    @packed : Slice(UInt8)?
    @verb : String

    def initialize(verb : String | Symbol, @uuid : UUID = UUID.new, @data : Array(String) = [] of String)
      @verb = symbol_to_string(verb)
    end

    def initialize(verb : String | Symbol, @uuid : String, @data : Array(String) = [] of String)
      @uuid = UUID.new(@uuid.as(String))
      @verb = symbol_to_string(verb)
    end

    def symbol_to_string(verb)
      case verb
      # The first element in the data portion of most frames should be the group ID.
      when :log
        "L"
      when :command
        "C"
      when :response
        "R"
      when :telemetry
        "T"
      when :query
        "Q"
      when :set
        "S"
      else
        verb.to_s
      end
    end

    def to_msgpack
      return @packed.not_nil! if @packed

      @packed = {@uuid.to_s, @verb, @data}.to_msgpack.not_nil!
    end
  end
end
