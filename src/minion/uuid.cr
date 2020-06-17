require "uuid"
require "random/isaac"

# For no good reason that I can discern, Time doesn't expose the internal seconds/nanoseconds representation.
# There are protected methods to get it, but we want it, so let's get it.
struct Time
  @[AlwaysInline]
  def internal_seconds
    @seconds
  end

  @[AlwaysInline]
  def internal_nanoseconds
    @nanoseconds
  end
end

module Minion
  # This struct wraps up a UUID that encodes a timestamp measured as seconds
  # from the epoch (0001-01-01 00:00:00.0 UTC) observed at the location where
  # the timestamp was generated, plus nanoseconds in the current second, plus
  # 6 bytes for unique identification of the source -- this could be an
  # IPV4 address with two null bytes, a MAC address, or some other sequence
  # that will fit in 6 bytes.
  #
  # Nanoseconds will fit in an Int32 (4 bytes), but seconds since the epoch
  # will not. The current number of seconds leaks a short distance into a
  # 5th byte, meaning that in this class, it has to be represented by an
  # Int64. This is problematic because a UID allows for 16 bytes, so the
  # use of 8 for seconds and 4 for nanoseconds leaves only 4 four system
  # identification. It also leaves three bytes in the UUID as zeros.
  #
  # One solution is to combine the seconds and the nanoseconds into a single
  # Int64 number. This requires math operations to do efficiently:
  #     (seconds * 1000000000) + nanoseconds
  # and then more math to extract the original numbers in order to reconstruct
  # the original timestamp. This leaves 8 bytes for identification or other
  # uniqueness information.
  #
  # The other options is to truncate 2 bytes off of the seconds, storing
  # 6 bytes of seconds data. This leaves 6 bytes for identification.
  #
  # The current implementation chose option #2, as it is less work to generate
  # a UUID if math is not involved.
  #
  # +-------------+-----------------+------------+
  # | nanoseconds |     seconds     | identifier |
  # |    0..3     |      4..10      |   11..15   |
  # +-------------+-----------------+------------+
  #
  struct UUID
    @@prng = Random::ISAAC.new
    @@string_matcher = /^(............)-(....)-(....)-(............)/
    class_property identifier
    class_property extra

    @bytes : Slice(UInt8) = Slice(UInt8).new(16)
    @seconds_and_nanoseconds : Tuple(Int64, Int32)?
    @timestamp : Time?
    @location : Time::Location = Time::Location.local

    def initialize(uuid : String)
      @bytes = uuid.tr("-", "").hexbytes
    end

    def initialize(seconds : Int64, nanoseconds : Int32, identifier : Slice(UInt8) | String | Nil = nil)
      _initialize(seconds, nanoseconds, identifier)
    end

    def _initialize(seconds : Int64, nanoseconds : Int32, identifier : Slice(UInt8) | String | Nil)
      id = if identifier.is_a?(String)
             buf = Slice(UInt8).new(6)
             number_of_bytes = identifier.size < 6 ? identifier.size : 6
             buf[0, number_of_bytes].copy_from(identifier.to_slice[0, number_of_bytes])
           else
             identifier
           end

      IO::ByteFormat::BigEndian.encode(seconds, @bytes[2, 8])
      IO::ByteFormat::BigEndian.encode(nanoseconds, @bytes[0, 4])
      @bytes[10, 6].copy_from(id || @@prng.random_bytes(6))
    end

    def initialize(timestamp : Time, identifier : Slice(UInt8) | String | Nil = nil)
      _initialize(timestamp.internal_seconds, timestamp.internal_nanoseconds, identifier)
    end

    def initialize(identifier : Slice(UInt8) | String | Nil = nil)
      t = Time.local
      _initialize(t.internal_seconds, t.internal_nanoseconds, identifier)
    end

    def seconds_and_nanoseconds : Tuple(Int64, Int32)
      sns = @seconds_and_nanoseconds
      return sns if !sns.nil?

      long_seconds = Slice(UInt8).new(8)
      long_seconds[2, 6].copy_from(@bytes[4, 6])
      @seconds_and_nanoseconds = {
        IO::ByteFormat::BigEndian.decode(Int64, long_seconds),
        IO::ByteFormat::BigEndian.decode(Int32, @bytes[0, 4]),
      }
    end

    def timestamp
      return @timestamp if @timestamp
      sns = seconds_and_nanoseconds
      puts sns.inspect
      @timestamp = Time.new(seconds: sns[0], nanoseconds: sns[1], location: @location)
    end

    def to_s
      hs = @bytes.hexstring
      "#{hs[0..11]}-#{hs[12..15]}-#{hs[16..19]}-#{hs[20..31]}"
    end
  end
end
