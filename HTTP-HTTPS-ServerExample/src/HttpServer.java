import java.net.InetSocketAddress;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;

public class HttpServer {

    static final int DEFAULT_NUM_ACCEPT_THREADS = 2;
    static final int DEFAULT_NUM_WORKER_THREADS = 6;
    static final int DEFAULT_MAX_REQUEST_SIZE = 4 * 1024 * 1024; // 4MB
    static final int DEFAULT_MAX_CHUNK_SIZE = 65536;
    static final int DEFAULT_IDLE_READ_TIMEOUT = 0;

    private final Channel channel;
    private final Channel httpsChannel;
    private final HttpServerHandler handler;

    /* hard-wired for now */
    private final int maxRequestSize;
    private final int maxChunkSize;

    /* How many seconds before an idle channel read to timeout */
    private final int idleReadTimeout;

    /*
     * bossGroup accepts incoming connections. The workerGroup handles data
     * requests on established connections. The parameter is number of
     * threads used in the event loop group.
     */
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private final SkLogger logger;

    public HttpServer(int httpPort,
                      int httpsPort,
                      int numAcceptThreads,
                      int numWorkerThreads,
                      RequestHandler requestHandler,
                      SslContext ctx,
                      SkLogger logger) throws InterruptedException {
        this(null /* default to localhost */, httpPort, httpsPort,
             numAcceptThreads, numWorkerThreads, 0 /* maxRequestSize */,
             0 /* maxChunkSize */, 0 /* readIdleTimeout */,
             requestHandler, ctx, logger);
    }

    public HttpServer(String httpHost,
                      int httpPort,
                      int httpsPort,
                      int numAcceptThreads,
                      int numWorkerThreads,
                      int maxRequestSize,
                      int maxChunkSize,
                      int idleReadTimeout,
                      RequestHandler requestHandler,
                      SslContext ctx,
                      SkLogger logger) throws InterruptedException {

        this.logger = logger;

        numAcceptThreads =
            (numAcceptThreads == 0 ?
                 DEFAULT_NUM_ACCEPT_THREADS : numAcceptThreads);
        numWorkerThreads =
            (numWorkerThreads == 0 ?
                 DEFAULT_NUM_WORKER_THREADS : numWorkerThreads);
        this.maxRequestSize =
            (maxRequestSize == 0 ?
                 DEFAULT_MAX_REQUEST_SIZE : maxRequestSize);
        this.maxChunkSize =
            (maxChunkSize == 0 ?
                 DEFAULT_MAX_CHUNK_SIZE : maxChunkSize);
        this.idleReadTimeout =
            (idleReadTimeout == 0 ?
                 DEFAULT_IDLE_READ_TIMEOUT : idleReadTimeout);

        bossGroup = new NioEventLoopGroup(numAcceptThreads);
        workerGroup = new NioEventLoopGroup(numWorkerThreads);

        handler = new HttpServerHandler(requestHandler, logger);

        /*
         * http and https are different channels that share workers and event
         * loops
         */

        channel =
            (httpPort != 0 ? createChannel(httpHost, httpPort, null) : null);

        if (httpsPort != 0 && ctx != null) {
            httpsChannel = createChannel(httpHost, httpsPort, ctx);
        } else {
            httpsChannel = null;
        }
    }

    /**
     * Shared code for creating channels based on http vs https
     */
    private Channel createChannel(String host, int port, SslContext sslCtx)
        throws InterruptedException {

        ServerBootstrap sb = new ServerBootstrap();
        sb.group(bossGroup, workerGroup)

            /* use a new NioServerSocketChannel instance for new channels */
            .channel(NioServerSocketChannel.class)

            /* use HttpServerInitializer to init new channels */
            .childHandler(new HttpServerInitializer(handler,
                                                    this,
                                                    sslCtx,
                                                    logger))

            /* set some socket options */
            .option(ChannelOption.SO_BACKLOG, 1024)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

        /*
         * Bind to specific host name
         */
        if (host != null) {
            return sb.bind(new InetSocketAddress(host, port)).sync().channel();
        }

        /* Bind and start to accept incoming connections */
        return sb.bind(port).sync().channel();
    }

    int getMaxRequestSize() {
        return maxRequestSize;
    }

    int getMaxChunkSize() {
        return maxChunkSize;
    }

    int getIdleReadTimeout() {
        return idleReadTimeout;
    }

    /**
     * Wait for shutdown
     */
    public HttpServer waitForShutdown() throws InterruptedException {
        if (channel != null) {
            channel.closeFuture().await();
        }
        if (httpsChannel != null) {
            httpsChannel.closeFuture().await();
        }
        return this;
    }

    /**
     * Cleanly shut down the server and threads.
     */
    public HttpServer shutdown() throws InterruptedException {
        if (channel != null) {
            channel.close();
        }
        if (httpsChannel != null) {
            httpsChannel.close();
        }
        logger.info("Shutting down HttpServer");
        workerGroup.shutdownGracefully();
        bossGroup.shutdownGracefully();
        waitForShutdown();
        return this;
    }

    /**
     * Return HTTP server's SkLogger
     */
    public SkLogger getLogger() {
        return logger;
    }
}
