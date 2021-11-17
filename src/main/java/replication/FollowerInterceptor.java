package replication;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;
import org.springframework.web.servlet.resource.ResourceHttpRequestHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

public class FollowerInterceptor extends HandlerInterceptorAdapter {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Value(Routing.ENABLE_ROUTING_TO_LEADER_ON_WRITING)
    private Boolean enableRoutingToLeaderOnWriting;
    @Value("${application.rules.print-request-forwards:1}")
    private Boolean printRequestForwards;
    @Value("${application.rules.body-max-printing-size:256}")
    private int bodyMaxPrintingSize;

    @Autowired
    private LeaderDiscoverer leaderDiscoverer;
    @Autowired
    private RestTemplate restTemplate;

    private BiPredicate<HttpServletRequest, HandlerMethod> invokeOnLeaderOnlyPredicate;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        final Pair<Boolean, URI> invokeLocallyOrElse = invokeLocallyOrElse(request, handler);
        if(invokeLocallyOrElse.getLeft()) {
            return true;
        }
        final URI leader = invokeLocallyOrElse.getRight();
        final URI uri = new URI(
                leader.getScheme(),
                leader.getAuthority(),
                path(leader.getPath(), request.getRequestURI()),
                request.getQueryString(),
                null
        );
        try {
            HttpHeaders headers = toProxyHeaders(request);
            byte[] body = Utils.toByteArray(request.getInputStream());
            if(printRequestForwards != null && printRequestForwards) {
                String characterEncoding = Utils.defaultIfBlank(request.getCharacterEncoding(), StandardCharsets.UTF_8.name());
                logger.info("[FORWARD] [{}] {}", uri, Utils.abbreviate(new String(body, characterEncoding), bodyMaxPrintingSize));
            }
            ResponseEntity<Resource> responseEntity = restTemplate.exchange(
                    uri.toString(),
                    HttpMethod.valueOf(request.getMethod()),
                    new HttpEntity<>(body, headers),
                    Resource.class,
                    request.getParameterMap()
            );
            response.setStatus(responseEntity.getStatusCodeValue());
            responseEntity.getHeaders().forEach((key, value) -> {
                value.forEach(element -> response.addHeader(key, element));
            });
            if(responseEntity.getBody() != null) {
                Utils.copy(responseEntity.getBody().getInputStream(), response.getOutputStream());
            }
        } catch (ResourceAccessException e) {
            throw new LeaderUnavailableException(uri.toString(), request.getMethod(), e);
        } catch (final RestClientResponseException e) {
            if(e instanceof HttpServerErrorException) {
                logger.error(Utils.abbreviate(e.getResponseBodyAsString(), bodyMaxPrintingSize), e);
            }
            response.setStatus(e.getRawStatusCode());
            if(e.getResponseHeaders() != null && !e.getResponseHeaders().isEmpty()) {
                Set<String> forwardingResponseHeaders = ImmutableSet.of("content-type");
                for (Map.Entry<String, List<String>> entry : e.getResponseHeaders().entrySet()) {
                    String key = entry.getKey();
                    if(forwardingResponseHeaders.contains(key.toLowerCase())) {
                        List<String> value = entry.getValue();
                        value.forEach(element -> response.addHeader(key, element));
                    }
                }
            }
            response.getOutputStream().write(e.getResponseBodyAsByteArray());
        }
        response.getOutputStream().flush();
        return false;
    }

    private Pair<Boolean, URI> invokeLocallyOrElse(HttpServletRequest request, Object handler) throws InterruptedException, ExecutionException, TimeoutException {
        if(enableRoutingToLeaderOnWriting == null || !enableRoutingToLeaderOnWriting) {
            return Pair.of(true, null);
        }
        if(handler instanceof ResourceHttpRequestHandler) {
            return Pair.of(true, null);
        }
        if (handler instanceof HandlerMethod) {
            boolean invokeOnLeaderOnly = false;
            final HandlerMethod handlerMethod = (HandlerMethod) handler;
            final Class<?> declaringClass = handlerMethod.getMethod().getDeclaringClass();
            if (handlerMethod.hasMethodAnnotation(RouteToLeader.class)) {
                if (!handlerMethod.getMethodAnnotation(RouteToLeader.class).exclusive()) {
                    invokeOnLeaderOnly = true;
                }
            } else if (declaringClass.isAnnotationPresent(RouteToLeader.class)) {
                if (!declaringClass.getAnnotation(RouteToLeader.class).exclusive()) {
                    invokeOnLeaderOnly = true;
                }
            } else if (this.invokeOnLeaderOnlyPredicate != null && this.invokeOnLeaderOnlyPredicate.test(request, handlerMethod)) {
                invokeOnLeaderOnly = true;
            }
            if(!invokeOnLeaderOnly) {
                return Pair.of(true, null);
            }
        }
        return leaderDiscoverer.isLeaderOrElse();
    }

    public void setInvokeOnLeaderOnlyPredicate(BiPredicate<HttpServletRequest, HandlerMethod> invokeOnLeaderOnlyPredicate) {
        this.invokeOnLeaderOnlyPredicate = invokeOnLeaderOnlyPredicate;
    }

    private static HttpHeaders toProxyHeaders(HttpServletRequest request) {
        final HttpHeaders httpHeaders = toHeaders(request);
        appendXForwardedFor(request, httpHeaders);
        return httpHeaders;
    }

    private static HttpHeaders toHeaders(HttpServletRequest request) {
        HttpHeaders headers = new HttpHeaders();
        for (Enumeration<String> headerNames = request.getHeaderNames(); headerNames.hasMoreElements();) {
            String headerName = headerNames.nextElement();
            for(Enumeration<String> headerValues = request.getHeaders(headerName); headerValues.hasMoreElements();) {
                String headerValue = headerValues.nextElement();
                headers.add(headerName, headerValue);
            }
        }
        return headers;
    }

    private static void appendXForwardedFor(HttpServletRequest request, HttpHeaders headers) {
        final String remoteAddr = request.getRemoteAddr();
        headers.add("X-Forwarded-For", remoteAddr);
    }

    private static String path(String... paths) {
        return Arrays.stream(paths)
                .map(path -> path.endsWith("/") ? path.substring(0, path.length() - 1) : path)
                .collect(Collectors.joining("/"));
    }
}

class LeaderUnavailableException extends Exception {
    public LeaderUnavailableException(String url, String method, Throwable t) {
        super(String.format("I/O error on %s request for \"%s\": leader unavailable", method, url), t);
    }
}