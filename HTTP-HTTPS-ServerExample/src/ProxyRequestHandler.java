
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static oracle.nosql.common.http.Constants.CONTENT_DISPOSITION;
import static oracle.nosql.common.http.Constants.CONTENT_DISPOSITION_VALUE;
import static oracle.nosql.common.http.Constants.CONTENT_LENGTH;
import static oracle.nosql.common.http.Constants.X_CONTENT_TYPE_OPTIONS;
import static oracle.nosql.common.http.Constants.X_CONTENT_TYPE_OPTIONS_VALUE;
import static oracle.nosql.common.http.Constants.X_FORWARDED_FOR;
import static oracle.nosql.common.http.Constants.X_REAL_IP;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;

public final class ProxyRequestHandler implements RequestHandler {

    final SkLogger logger;
    final Map<String, Service> services;
    final LogControl logControl;

    public ProxyRequestHandler(LogControl logControl, final SkLogger logger) {
        this.logger = logger;
        this.logControl = logControl;
        services = new HashMap<String, Service>();
    }

    public void addService(String name, Service service) {
        services.put(name, service);
    }

    public Service getService(String name) {
        return services.get(name);
    }

    @Override
    public FullHttpResponse handleRequest(FullHttpRequest request,
                                          ChannelHandlerContext ctx) {

        FullHttpResponse response;
        try {
            final String path = new URI(request.uri()).getPath();

            Service service = findService(path);
            if (service == null) {
                HttpHeaders headers = request.headers();
                final CharSequence realIp = headers.get(X_REAL_IP);
                final CharSequence forwardedFor =
                    headers.get(X_FORWARDED_FOR);
                final String remoteAddr =
                    ctx.channel().remoteAddress().toString();
                StringBuilder sb = new StringBuilder();

                sb.append("Cannot find service for path ").append(path)
                    .append(", remote address=").append(remoteAddr);
                if (realIp != null) {
                    sb.append(", ").append(X_REAL_IP).append("=")
                        .append(realIp);
                }
                if (forwardedFor != null) {
                    sb.append(", ").append(X_FORWARDED_FOR).append("=")
                        .append(forwardedFor);
                }

                /*
                 * TODO:
                 * o consider rate-limiting logger if there are lot of these
                 * o consider hard-close of connection if this looks like an
                 * attack.
                 */
                logger.info(sb.toString());
                return badResponse();
            }

            LogContext lc =logControl.generateLogContext
                (request.method().name() + " " + path);

            response = service.handleRequest(request, ctx, lc);

        } catch (Exception e) {
            logger.info("Exception handling request: " + e.getMessage());
            /*
             * In general services must handle their own internal exceptions and
             * map them to a reasonable FullHttpResponse.
             *
             * TODO: some exceptions may indicate that the channel needs to be
             * closed. Look into this.
             */
            // TODO: use exception message.
            response = badResponse();
        }
        addRequiredHeaders(response);
        return response;
    }

    private static FullHttpResponse badResponse() {
        FullHttpResponse response =
            new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST);
        response.headers().set(CONTENT_LENGTH, 0);
        return response;
    }

    /**
     * These headers are required by Oracle's security policies
     */
    private void addRequiredHeaders(FullHttpResponse response) {
        response.headers().set(X_CONTENT_TYPE_OPTIONS,
                               X_CONTENT_TYPE_OPTIONS_VALUE);
        response.headers().set(CONTENT_DISPOSITION,
                               CONTENT_DISPOSITION_VALUE);
    }

    /**
     * Locate a registered Service instance based on the URI path.
     * This is not as efficient as a hash, but given that there are relatively
     * few services expected it is reasonable. Services should implement their
     * lookupService methods in an efficient manner.
     */
    private Service findService(String uri) {
        /*
         * Strip leading "/" if present
         */
        String name = (uri.startsWith("/") ? uri.substring(1) : uri);

        for (Service service : services.values()) {
            if (service.lookupService(name)) {
                return service;
            }
        }
        return null;
    }
}
