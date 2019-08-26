
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.ReadTimeoutHandler;
import oracle.nosql.common.sklogger.SkLogger;

final class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
    private static final String CODEC_HANDLER_NAME = "http-codec";
    private static final String AGG_HANDLER_NAME = "http-aggregator";
    private static final String HTTP_HANDLER_NAME = "http-server-handler";
    private static final String READ_TIMEOUT_HANDLER_NAME =
        "http-read-timeout-handler";

    private final HttpServerHandler handler;
    private final int maxChunkSize;
    private final int maxRequestSize;
    private final int idleReadTimeout;
    private final SkLogger logger;
    private final SslContext sslCtx;

    public HttpServerInitializer(HttpServerHandler handler,
                                 HttpServer server,
                                 SslContext sslCtx,
                                 SkLogger logger) {

        this.handler = handler;
        this.logger = logger;
        this.maxRequestSize = server.getMaxRequestSize();
        this.maxChunkSize = server.getMaxChunkSize();
        this.idleReadTimeout = server.getIdleReadTimeout();
        this.sslCtx = sslCtx;
    }

    /**
     * Initialize a channel with handlers that:
     * 1 -- handle and HTTP
     * 2 -- handle chunked HTTP requests implicitly, only calling channelRead
     * with FullHttpRequest.
     * 3 -- the request handler itself
     *
     * TODO: HttpContentCompressor, other options?
     */
    @Override
    public void initChannel(SocketChannel ch) {
        logger.fine("HttpServiceInitializer, initializing new channel");
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc()));
        }
        p.addLast(READ_TIMEOUT_HANDLER_NAME,
                  new ReadTimeoutHandler(idleReadTimeout));
        p.addLast(CODEC_HANDLER_NAME,
                  new HttpServerCodec(4096, // initial line
                                      8192, // header size
                                      maxChunkSize)); // chunksize
        p.addLast(AGG_HANDLER_NAME,
                  new HttpObjectAggregator(maxRequestSize));
        p.addLast(HTTP_HANDLER_NAME, handler);
    }
}
