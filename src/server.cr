require "http/server"
require "./storage"

storage = Storage.new

server = HTTP::Server.new do |ctx|
    if ctx.request.method == "POST"
        body = ctx.request.body
        if body
            body_s = body.gets_to_end
            storage.write body_s
            ctx.response.print body_s
            ctx.response.print '\n'
            next
        end
    end
    if ctx.request.method == "GET"
      ctx.response.print storage.read
      next
    end
    # ctx.response.content_type = "text/json"
    ctx.response.content_type = "text/plain"
    ctx.response.print "default response #{Time.local}"
end

address = server.bind_tcp 5000
puts "listening on #{address}"

Signal::INT.trap do
    storage.close
    server.close
end

server.listen
