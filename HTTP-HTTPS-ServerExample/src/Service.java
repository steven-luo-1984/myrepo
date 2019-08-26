
import oracle.nosql.common.contextlogger.LogContext;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;

public interface Service {

    /**
     * Handles a request
     *
     * TODO: it'd be nice to abstract out HTTP 1.1 vs HTTP 2 but the Netty
     * objects involved are different. When we want to handle both, think about
     * better abstractions. Maybe just add an HTTP 2-based handleRequest method.
     */
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx,
                                          LogContext lc);

    /**
     * Returns true if the uri/path indicates this service
     *
     * TODO: should the method be passed as well? For now, the service will
     * deal with support of specific methods or not.
     */
    public boolean lookupService(String uri);
}
