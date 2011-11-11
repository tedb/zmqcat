require 'logger'
require 'rubygems'
require 'trollop'
require 'zmq'

class ZMQcat
  attr_accessor :log, :opts, :context
  
  def initialize(arguments = [])
    @opts = get_options(arguments)
    
    @eof = ''
    
    STDOUT.sync = true if opts[:sync]
    STDERR.sync = true if opts[:sync]
  
    @log = Logger.new(STDERR)
    log.level = opts[:debug] ? Logger::DEBUG : Logger::WARN
    log.debug "Debug messages are ENABLED"

    @context = ZMQ::Context.new(1)
    log.debug "Created ZMQ context"

    log.debug "Running socket_type #{opts[:socket_type]}"
    begin
      send opts[:socket_type]
    ensure
      @context.close
    end

    #log.close
  end

  def get_options(arguments)
    opts = Trollop::options(arguments) do
      version "zmqcat.rb 0.01 (c) 2011 SPARC, LLC"
      banner <<-EOS
zmqcat is Netcat for the ZeroMQ library.  Pipe from STDIN or to STDOUT; each line is a ZeroMQ message.

Usage:
      zmqcat [options]
where [options] are:
      EOS
  
      opt :connect, "Be a client; specify URL to connect to; defaults to tcp://127.0.0.1:9000", :type => String, :default => "tcp://127.0.0.1:9000"
      opt :bind, "Be a server; specify URL to listen to", :type => String

      opt :upstream, "Pipeline pattern; be downstream, receiving data from an upstream node."
      opt :downstream, "Pipeline pattern; be upstream, sending data to a downstream node."
      opt :pub, "Publish-subscribe pattern; send data to subscribers."
      opt :sub, "Publish-subscribe pattern; receive data from publishers."
      #opt :pair_recv "Exclusive pair pattern; "
      #opt :pair_send "Exclusive pair pattern; "
      
      opt :one, "Only send or receive one message.  Use for binary objects.  Sending will read STDIN until EOF and send as one message (e.g., an image file), then exit.  Receiving will write one message to STDOUT and exit."
      opt :continue, "Don't exit when the sender reaches EOF (wait for another instance)"
      opt :sync, "Flush STDOUT and STDERR after every line"
      opt :debug, "Print debug logging to STDERR"
      
      conflicts :upstream, :downstream, :pub, :sub
      conflicts :connect, :bind
    end

    #Trollop::die :volume, "must be non-negative" if opts[:volume] < 0
    #Trollop::die :file, "must exist" unless File.exist?(opts[:file]) if opts[:file]

    opts[:socket_type] = [:upstream, :downstream, :pub, :sub].select { |type| opts[type] == true }.first
    raise ArgumentError, "Argument must specify one of --upstream, --downstream, --pub, or --sub" unless opts[:socket_type]
  
    return opts
  end

  def upstream
    inbound = context.socket(ZMQ::UPSTREAM)
    begin
      log.debug "We are downstream (reading inbound); got socket"
      bind_or_connect(inbound)
    
      # write each message as-is (no newline)
      loop do
        received = inbound.recv
        return if received == @eof unless opts[:continue]
        print received
        return true if opts[:one]
      end
    ensure
      inbound.close
    end
  end

  def downstream
    outbound = context.socket(ZMQ::DOWNSTREAM)
    begin
      log.debug "We are upstream (sending outbound); got socket"
      bind_or_connect(outbound)
    
      if opts[:one]
        log.debug "Sending contents of input #{ARGF.filename}"
        ARGF.binmode
        outbound.send ARGF.read
        outbound.send @eof
      else
        ARGF.each_with_index do |line, idx|
          log.debug "Sending line #{idx} for input #{ARGF.filename}"
          outbound.send line
        end
        outbound.send @eof
      end
    ensure
      outbound.close
    end
  end

  def pub
  end

  def sub
  end
  
  def bind_or_connect(endpoint)
    if opts[:bind]
      log.debug "Binding on #{opts[:bind]}..."
      endpoint.bind opts[:bind]
      log.debug "Bind set up."
      
    elsif opts[:connect]
      log.debug "Connecting to #{opts[:connect]}..."
      endpoint.connect opts[:connect]
      log.debug "Connect set up."
    end
    return nil
  end
end

ZMQcat.new(ARGV)