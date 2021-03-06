require "redis/connection/registry"
require "redis/connection/command_helper"
require "redis/errors"
require "socket"

class Redis
  module Connection
    module SocketMixin

      CRLF = "\r\n".freeze

      def initialize(*args)
        super(*args)

        @timeout = nil
        @monitoring_thread = nil
        @writer = nil
        @buffer = ""
      end

      def timeout=(timeout)
        if timeout && timeout > 0
          @timeout = timeout
        else
          @timeout = nil
        end
      end

      def read(nbytes)
        result = @buffer.slice!(0, nbytes)

        while result.bytesize < nbytes
          result << _read_from_socket(nbytes - result.bytesize)
        end

        result
      end

      def gets
        crlf = nil

        while (crlf = @buffer.index(CRLF)) == nil
          @buffer << _read_from_socket(1024)
        end

        @buffer.slice!(0, crlf + CRLF.bytesize)
      end

      def _read_from_socket(nbytes)
        begin
          read_nonblock(nbytes)
        rescue Errno::EWOULDBLOCK, Errno::EAGAIN => ex
          retry if monitor_socket_fd
        end

      rescue EOFError
        raise Errno::ECONNRESET
      end

      def monitor_socket_fd
        reader, @writer = IO.pipe

        begin
          @monitoring_thread = Thread.new do
            begin
              @writer.write(IO.select([self], nil, nil, @timeout))
            ensure
              @writer.close
            end
          end

          select_value = reader.read
          select_value != "" ? true : (raise Redis::TimeoutError)
        ensure
          reader.close
        end
      end

      def close
        @monitoring_thread.terminate if @monitoring_thread
        @writer.close if @writer && !@writer.closed?
        super
      end
    end

    if defined?(RUBY_ENGINE) && RUBY_ENGINE == "jruby"

      require "timeout"

      class TCPSocket < ::TCPSocket

        include SocketMixin

        def self.connect(host, port, timeout)
          Timeout.timeout(timeout) do
            sock = new(host, port)
            sock
          end
        rescue Timeout::Error
          raise TimeoutError
        end
      end

      if defined?(::UNIXSocket)

        class UNIXSocket < ::UNIXSocket

          include SocketMixin

          def self.connect(path, timeout)
            Timeout.timeout(timeout) do
              sock = new(path)
              sock
            end
          rescue Timeout::Error
            raise TimeoutError
          end

          # JRuby raises Errno::EAGAIN on #read_nonblock even when IO.select
          # says it is readable (1.6.6, in both 1.8 and 1.9 mode).
          # Use the blocking #readpartial method instead.

          def _read_from_socket(nbytes)
            readpartial(nbytes)

          rescue EOFError
            raise Errno::ECONNRESET
          end
        end

      end

    else

      class TCPSocket < ::Socket

        include SocketMixin

        def self.connect_addrinfo(ai, port, timeout)
          sock = new(::Socket.const_get(ai[0]), Socket::SOCK_STREAM, 0)
          sockaddr = ::Socket.pack_sockaddr_in(port, ai[3])

          begin
            sock.connect_nonblock(sockaddr)
          rescue Errno::EINPROGRESS
            if IO.select(nil, [sock], nil, timeout) == nil
              raise TimeoutError
            end

            begin
              sock.connect_nonblock(sockaddr)
            rescue Errno::EISCONN
            end
          end

          sock
        end

        def self.connect(host, port, timeout)
          # Don't pass AI_ADDRCONFIG as flag to getaddrinfo(3)
          #
          # From the man page for getaddrinfo(3):
          #
          #   If hints.ai_flags includes the AI_ADDRCONFIG flag, then IPv4
          #   addresses are returned in the list pointed to by res only if the
          #   local system has at least one IPv4 address configured, and IPv6
          #   addresses are returned only if the local system has at least one
          #   IPv6 address configured. The loopback address is not considered
          #   for this case as valid as a configured address.
          #
          # We do want the IPv6 loopback address to be returned if applicable,
          # even if it is the only configured IPv6 address on the machine.
          # Also see: https://github.com/redis/redis-rb/pull/394.
          addrinfo = ::Socket.getaddrinfo(host, nil, Socket::AF_UNSPEC, Socket::SOCK_STREAM)

          # From the man page for getaddrinfo(3):
          #
          #   Normally, the application should try using the addresses in the
          #   order in which they are returned. The sorting function used
          #   within getaddrinfo() is defined in RFC 3484 [...].
          #
          addrinfo.each_with_index do |ai, i|
            begin
              return connect_addrinfo(ai, port, timeout)
            rescue SystemCallError
              # Raise if this was our last attempt.
              raise if addrinfo.length == i+1
            end
          end
        end
      end

      class UNIXSocket < ::Socket

        include SocketMixin

        def self.connect(path, timeout)
          sock = new(::Socket::AF_UNIX, Socket::SOCK_STREAM, 0)
          sockaddr = ::Socket.pack_sockaddr_un(path)

          begin
            sock.connect_nonblock(sockaddr)
          rescue Errno::EINPROGRESS
            if IO.select(nil, [sock], nil, timeout) == nil
              raise TimeoutError
            end

            begin
              sock.connect_nonblock(sockaddr)
            rescue Errno::EISCONN
            end
          end

          sock
        end
      end

    end

    class Ruby
      include Redis::Connection::CommandHelper

      MINUS    = "-".freeze
      PLUS     = "+".freeze
      COLON    = ":".freeze
      DOLLAR   = "$".freeze
      ASTERISK = "*".freeze

      def self.connect(config)
        if config[:scheme] == "unix"
          sock = UNIXSocket.connect(config[:path], config[:timeout])
        else
          sock = TCPSocket.connect(config[:host], config[:port], config[:timeout])
        end

        instance = new(sock)
        instance.timeout = config[:timeout]
        instance.set_tcp_keepalive config[:tcp_keepalive]
        instance
      end

      if [:SOL_SOCKET, :SO_KEEPALIVE, :SOL_TCP, :TCP_KEEPIDLE, :TCP_KEEPINTVL, :TCP_KEEPCNT].all?{|c| Socket.const_defined? c}
        def set_tcp_keepalive(keepalive)
          return unless keepalive.is_a?(Hash)

          @sock.setsockopt(Socket::SOL_SOCKET, Socket::SO_KEEPALIVE,  true)
          @sock.setsockopt(Socket::SOL_TCP,    Socket::TCP_KEEPIDLE,  keepalive[:time])
          @sock.setsockopt(Socket::SOL_TCP,    Socket::TCP_KEEPINTVL, keepalive[:intvl])
          @sock.setsockopt(Socket::SOL_TCP,    Socket::TCP_KEEPCNT,   keepalive[:probes])
        end

        def get_tcp_keepalive
          {
            :time   => @sock.getsockopt(Socket::SOL_TCP, Socket::TCP_KEEPIDLE).int,
            :intvl  => @sock.getsockopt(Socket::SOL_TCP, Socket::TCP_KEEPINTVL).int,
            :probes => @sock.getsockopt(Socket::SOL_TCP, Socket::TCP_KEEPCNT).int,
          }
        end
      else
        def set_tcp_keepalive(keepalive)
        end

        def get_tcp_keepalive
          {
          }
        end
      end

      def initialize(sock)
        @sock = sock
      end

      def connected?
        !! @sock
      end

      def disconnect
        @sock.close
      rescue
      ensure
        @sock = nil
      end

      def timeout=(timeout)
        if @sock.respond_to?(:timeout=)
          @sock.timeout = timeout
        end
      end

      def write(command)
        @sock.write(build_command(command))
      end

      def read
        line = @sock.gets
        reply_type = line.slice!(0, 1)
        format_reply(reply_type, line)

      rescue Errno::EAGAIN
        raise TimeoutError
      end

      def format_reply(reply_type, line)
        case reply_type
        when MINUS    then format_error_reply(line)
        when PLUS     then format_status_reply(line)
        when COLON    then format_integer_reply(line)
        when DOLLAR   then format_bulk_reply(line)
        when ASTERISK then format_multi_bulk_reply(line)
        else raise ProtocolError.new(reply_type)
        end
      end

      def format_error_reply(line)
        CommandError.new(line.strip)
      end

      def format_status_reply(line)
        line.strip
      end

      def format_integer_reply(line)
        line.to_i
      end

      def format_bulk_reply(line)
        bulklen = line.to_i
        return if bulklen == -1
        reply = encode(@sock.read(bulklen))
        @sock.read(2) # Discard CRLF.
        reply
      end

      def format_multi_bulk_reply(line)
        n = line.to_i
        return if n == -1

        Array.new(n) { read }
      end
    end
  end
end

Redis::Connection.drivers << Redis::Connection::Ruby
