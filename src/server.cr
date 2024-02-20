require "http/server"
require "./storage"


class Server
  @address : Socket::IPAddress | Nil

  def initialize()
    @storage = Storage.new

    @server = HTTP::Server.new do |ctx|
      self.handler(ctx)
    end

    @address = @server.not_nil!.bind_tcp 5000

    Signal::INT.trap do
      @storage.close
      @server.not_nil!.close
    end
  end

  def handler(ctx)
    if ctx.request.method == "POST"
      body = ctx.request.body
      if body
        body_s = body.gets_to_end
        @storage.write body_s
        ctx.response.print body_s
        ctx.response.print '\n'
        return
      end
    end
    if ctx.request.method == "GET"
      ctx.response.print @storage.read
      return
    end
    # ctx.response.content_type = "text/json"
    ctx.response.content_type = "text/plain"
    ctx.response.print "default response #{Time.local}"
  end

  def listen()
    puts "listening on #{@address}"
    @server.not_nil!.listen
  end
end
