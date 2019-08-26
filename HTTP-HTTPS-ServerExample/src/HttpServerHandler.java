import static oracle.nosql.common.http.Constants.CONNECTION;
import static oracle.nosql.common.http.Constants.KEEP_ALIVE;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.ReferenceCountUtil;

@Sharable
final class HttpServerHandler extends ChannelInboundHandlerAdapter {

    private final RequestHandler handler;
    private final SkLogger logger;

    HttpServerHandler(RequestHandler handler, SkLogger logger) {
        this.handler = handler;
        this.logger = logger;
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /**
     * Handles a data request. This handler requires a FullHttpRequest as
     * constructed by an HttpObjectAggregator in the pipeline.
     *
     * TODO: should keepalive move to the handler?
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof FullHttpRequest) {

                FullHttpRequest req = (FullHttpRequest) msg;

                final boolean keepAlive = HttpUtil.isKeepAlive(req);

                /*
                 * Handle the request
                 *
                 * NOTE: should this be handed off for fully async proxy,
                 * keeping the event loop short?
                 */
                FullHttpResponse response = handler.handleRequest(req, ctx);

                if (keepAlive) {
                    response.headers().set(CONNECTION, KEEP_ALIVE);
                }

                final ChannelFuture f = ctx.writeAndFlush(response);
                f.addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture future) {
                            if (!keepAlive) {
                                /*
                                 * This is not generally the case. The cloud
                                 * driver sets keep-alive to re-use connections
                                 */
                                ctx.close();
                            }
                        }
                    });
            } else {
                logger.warning("HttpServerHandler channelRead not instance of "
                               + "FullHttpRequest: " + msg.getClass());
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {

        /* note idle channel timeouts in the log */
        if (cause instanceof ReadTimeoutException) {
            logger.fine(
                "HttpServerHandler idle channel timeout, closing channel: " +
                ctx.channel() + ": " + cause.getMessage());
        } else {
            logger.info("HttpServerHandler exception caught, closing channel: " +
                        ctx.channel() + ", cause: " + cause.getMessage());
        }
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.fine("HttpServerHandler inactive channel: " +
                    ctx.channel());
        /* should the context be closed? */
        ctx.close();
    }
}
