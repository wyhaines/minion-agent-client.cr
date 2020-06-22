# This is an interactive minion repl shell. It is a shell for interacting with Minion manually.

require "option_parser"
require "fancyline"
require "./client"
require "benchmark"

VERSION  = "0.1.0"
CONFIG   = {} of String => String | Int32
COMMANDS = {
  "log":       "L",
  "command":   "C",
  "response":  "R",
  "telemetry": "T",
  "query":     "Q",
  "set":       "S",
}

OptionParser.new do |opts|
  opts.banner = "IAR Interactive Agent REPL v#{VERSION}\nUsage: iar [options]"
  opts.separator ""

  opts.on("--host [HOST:PORT]", "The host and the port where the stream server to connect to is found.") do |server|
    parts = server.split(/:/, 2)
    if parts.size > 1
      host, port = parts
      CONFIG["host"] = host
      CONFIG["port"] = port
    else
      if parts[0] =~ /^\s*\d+\s*/
        CONFIG["host"] = "127.0.0.1"
        CONFIG["port"] = parts[0].to_i
      else
        CONFIG["host"] = parts[0].strip
        CONFIG["port"] = 47990
      end
    end
  end

  opts.on("-g", "--group [ID]", "The group or organization ID to use when talking to the stream server.") do |id|
    CONFIG["group"] = id
  end

  opts.on("-s", "--server [ID]", "The UUID to use to identify this server. If one is not provided, the client will generate one.") do |id|
    begin
      CONFIG["server"] = Minion::UUID.new(id)
    rescue ex
    end
  end

  opts.on("-k", "--key [KEY]", "The key to use along with the group ID to authenticate to the stream server.") do |key|
    CONFIG["key"] = key
  end

  opts.on("--help", "Show this help") do
    puts opts
    exit
  end

  opts.on("-v", "--version", "Show the current version of StreamServer.") do
    puts "IAR v#{VERSION}"
    exit
  end

  opts.invalid_option do |flag|
    STDERR.puts "Error: #{flag} is not a valid option."
    STDERR.puts opts
    exit(1)
  end
end.parse

CONFIG["host"] = "127.0.0.1" unless CONFIG.has_key?("host")
CONFIG["port"] = 47990 unless CONFIG.has_key?("port")
CONFIG["group"] = "" unless CONFIG.has_key?("group")
CONFIG["server"] = Minion::UUID.new unless CONFIG.has_key?("server")
CONFIG["key"] = "" unless CONFIG.has_key?("key")

streamserver = Minion::Client.new(
  host: CONFIG["host"].to_s,
  port: CONFIG["port"].to_i,
  group: CONFIG["group"].to_s,
  server: CONFIG["server"].to_s,
  key: CONFIG["key"].to_s)

fancy = Fancyline.new
puts "Messages to the StreamServer are in the format of:\nVERB::DATA1::DATA2::DATAn\nType 'verbs' for a list of known verbs\nType 'exit' or press CTRL-d to exit.\n"

colors = {
  "data":      :white,
  "verb":      :green,
  "highlight": :yellow,
  "error":     :light_red,
}

fancy.display.add do |context, line, yielder|
  if line =~ /^(\s*\d+\s+times\s*)\{(.*?)\}\s*$/
    prefix = $1.colorize(:light_green)
    line = $2
  else
    prefix = nil
  end

  if line && line =~ /\b\s*::\s*/
    verb, data = line.split(/::/, 2)
    color_verb = (COMMANDS.has_key?(verb) || COMMANDS.values.includes?(verb)) ? verb.colorize(colors["verb"]) : verb.colorize(colors["error"])

    color_parts = Array(Colorize::Object(String)).new(1)
    if ["log", "L", "telemetry", "T", "response", "R"].includes?(verb)
      parts = data.split(/::/)
      color_parts << parts[0].colorize(colors["highlight"])

      if parts.size > 1
        color_parts = color_parts + parts[1..-1].map { |d| d.colorize(colors["data"]) }
      end
    else
      # new_parts = data.split(/::/).map {|d| d.colorize(colors["data"])}
      color_parts = data.split(/::/).map { |d| d.colorize(:cyan) }
    end
    if prefix
      line = "#{prefix}{#{color_verb}::#{color_parts.join("::")}}"
    else
      line = "#{color_verb}::#{color_parts.join("::")}"
    end
  end
  yielder.call context, line
end

while input = fancy.readline("$ ")
  exit if input =~ /^\s*exit\s*$/
  if input =~ /^\s*help\s*$/
    puts <<-EHELP
      log (L)       -- log::SERVICE::MESSAGE
                       log::stderr::This log will be sent to the "stderr" service.
      telemetry (T) -- telemetry::TYPE::VALUE1::VALUE2::VALUEn
                       telemetry::loadavg::0.52::0.58::0.59
      response (R)  -- response::COMMANDID::TEXT
                       response::26d30cad-a07b-4aab-8f6e-52158ec73121:: 09:37:58 up 1 day,  9:00,  0 users,  load average: 0.52, 0.58, 0.59

      Built in benchmarking support:
      1000000 times {log::stderr::This is a testing log message.}
      EHELP
  else
    repeat = 1
    if input =~ /^\s*(\d+)\s+times\s*\{(.*?)\}\s*$/
      repeat = $1.to_i
      input = $2
    end

    parts = input.split(/::/)
    verb = parts[0]
    verb = COMMANDS[verb] if COMMANDS.has_key?(verb)

    data = parts.size > 1 ? parts[1..-1] : [] of String
    STDERR.puts "Sending: verb: #{verb}, data: #{data.inspect}"
    if repeat > 1
      Benchmark.bm do |bm|
        bm.report("#{repeat} iterations") do
          repeat.times do |n|
            streamserver.send(verb: verb, data: data)
          end
        end
      end
    else
      streamserver.send(verb: verb, data: data)
    end
  end
end
