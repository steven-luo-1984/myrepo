
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

public interface RequestHandler {

    /**
     * Handles a request
     *
     * @param request the incoming request
     * @param ctx context used for consolidation of resource allocation
     *
     * The implementing class must return a FullHttpResponse.
     *
     * TODO: can the ctx be avoided?
     */
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx);
}
