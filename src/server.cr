require "http/server"
STORAGE_FILE = ".data"

# https://crystal-lang.org/api/1.11.2/File.html#constructor-detail
storage = File.open STORAGE_FILE, mode: "a+"

server = HTTP::Server.new do |ctx|
    if ctx.request.method == "POST"
        body = ctx.request.body
        if body
            body_s = body.gets_to_end
            # File.write STORAGE_FILE, body_s + "\n", mode: "a"
            storage.print body_s + "\n" # appends because of mode
            storage.flush # maybe move to SIGINT trap
            ctx.response.print body_s
            ctx.response.print '\n'
            next
        end
    end
    if ctx.request.method == "GET"
        # ctx.response.print storage.gets_to_end # consumes buffer
        ctx.response.print File.read STORAGE_FILE
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
