package replication;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.unit.DataSize;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Conditional(Routing.class)
public class ReplicableInstanceFactory {

    private static final Logger log = LoggerFactory.getLogger(ReplicableInstanceFactory.class);

    @Value("${application.rules.compression-threshold:}")
    private DataSize compressionThreshold;
    @Value(Routing.ENABLE_ROUTING_TO_LEADER_ON_WRITING)
    private Boolean enableRoutingToLeaderOnWriting;

    @Autowired
    private LeaderDiscoverer leaderDiscoverer;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private StreamUtils streamUtils;

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
            1, 1,
            1L, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(),
            new CustomizableThreadFactory("notify-deliver")
    );
    private final ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(
            16,
            new CustomizableThreadFactory("ttl-detector")
    );

    ReplicableInstanceFactory() {
        executor.allowCoreThreadTimeOut(true);
        scheduledExecutor.setKeepAliveTime(1, TimeUnit.MINUTES);
        scheduledExecutor.allowCoreThreadTimeOut(true);
    }

    public <T> T newInstance(ReplicationInstanceParameter<T> parameter) {
        checkTargetModifiersAndTtlExpression(parameter.getDelegate());

        Object lock = MoreObjects.firstNonNull(parameter.getLock(), new Object());

        ProxyFactory pf = new ProxyFactory();
        pf.setTargetClass(parameter.getDelegate().getClass());
        pf.setTarget(parameter.getDelegate());
        pf.addInterface(ReplicableHolder.class);
        pf.setProxyTargetClass(true);
        pf.addAdvice((MethodInterceptor) invocation -> {
            final Method method = invocation.getMethod();
            if(isEnableRoutingToLeaderOnWriting() && method.isAnnotationPresent(Replicable.class)) {
                final Replicable replicable = method.getAnnotation(Replicable.class);
                if(!replicable.exclusive()) {
                    if(isLeader()) {
                        synchronized (lock) {
                            Object result = invocation.proceed();
                            try {
                                if(replicable.reset()) {
                                    resetEvents();
                                }
                                notifyEvent(invocation, getTtlInSeconds(method));
                            } catch (Exception e) {
                                log.error("", e);
                            }
                            return result;
                        }
                    } else {
                        synchronized (lock) {
                            return invocation.proceed();
                        }
                    }
                } else {
                    if(isLeader()) {
                        return invocation.proceed();
                    } else {
                        return null;
                    }
                }
            } else {
                return invocation.proceed();
            }
        });

        return (T) pf.getProxy();
    }


    private void resetEvents() {
        executor.getQueue().clear();
        String key = streamUtils.getKey();
        streamUtils.batchKvs(key, records -> {
            String[] recordIds = records.stream()
                    .map(r -> r.getId().getValue())
                    .toArray(String[]::new);
            stringRedisTemplate.boundStreamOps(key).delete(recordIds);
        });
    }

    private void notifyEvent(MethodInvocation invocation, long ttlInSeconds) {
        CompletableFuture<String> actionFuture = CompletableFuture.supplyAsync(() -> {
            try {
                final Action action = Action.newAction(
                        Utils2.toReplicableString(invocation.getMethod()),
                        invocation.getArguments(),
                        compressionThreshold
                );
                final ObjectMapper mapper = newObjectMapper();
                return mapper.writeValueAsString(action);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        Callable<String> c = () -> {
            final String action = actionFuture.get();
            if(action != null) {
                Map<String, String> payload = Utils.map(
                        "payload", action,
                        "ttl", Long.toString(ttlInSeconds),
                        "createTime", new SimpleDateFormat("yyyyMMdd HHmmss.SSS").format(new Date()),
                        "creator", streamUtils.getGroup()
                );
                String key = streamUtils.getKey();
                final RecordId recordId = stringRedisTemplate.opsForStream().add(key, payload);
                if(ttlInSeconds > 0) {
                    scheduledExecutor.schedule(() -> stringRedisTemplate.opsForStream().delete(key, recordId), ttlInSeconds, TimeUnit.SECONDS);
                }
            }
            return action;
        };
        executor.submit(c);
    }

    private boolean isEnableRoutingToLeaderOnWriting() {
        return enableRoutingToLeaderOnWriting != null && enableRoutingToLeaderOnWriting;
    }

    private boolean isLeader() throws InterruptedException, ExecutionException, TimeoutException {
        return leaderDiscoverer.isLeaderDiscoveryEnabled() && leaderDiscoverer.isLeaderOrElse().getLeft();
    }

    private static long getTtlInSeconds(Method method) {
        long ttl = method.getAnnotation(Replicable.class).ttl();
        if(ttl <= 0) {
            String ttlExpression = method.getAnnotation(Replicable.class).ttlExpression();
            if(Utils.isNotBlank(ttlExpression)) {
                ttl = DurationStyle.detectAndParse(ttlExpression).getSeconds();
            }
        }
        return ttl;
    }

    private static void checkTargetModifiersAndTtlExpression(Object target) {
        final Collection<Method> replicableMethods = Utils2.getMethods(target.getClass()).stream()
                .filter(method -> method.isAnnotationPresent(Replicable.class))
                .collect(Collectors.toCollection(LinkedList::new));

        replicableMethods.stream()
                .filter(method -> {
                    final int modifiers = method.getModifiers();
                    return Modifier.isAbstract(modifiers) || Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers);
                })
                .findAny()
                .ifPresent(method -> {
                    String message = String.format(
                            "'%s' (with modifier(s) 'abstract', 'private' or 'static') cannot be replicated",
                            Utils2.toReplicableString(method)
                    );
                    throw new IllegalArgumentException(message);
                });

        replicableMethods.forEach(method -> {
            final String ttlExpression = method.getAnnotation(Replicable.class).ttlExpression();
            if(Utils.isNotBlank(ttlExpression)) {
                try {
                    DurationStyle.detect(ttlExpression);
                } catch (IllegalArgumentException e) {
                    String message = String.format("'%s' on '%s' is not a valid duration", ttlExpression, Utils2.toReplicableString(method));
                    throw new IllegalArgumentException(message);
                }
            }
        });
    }

    static ObjectMapper newObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper;
    }

}

class Action {
    private static final String VERSION = "1.0";

    private String method;
    private Object[] parameters;
    private String[] parameterTypes;
    private byte[] compressedParameters;
    private String time;

    private Action() {
    }

    public static Action newAction(String method, Object[] parameters, DataSize compressionThreshold) throws IOException {
        Action action = new Action();
        action.method = method;
        action.time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd HHmmss.SSS"));
        final byte[] bytes = ReplicableInstanceFactory.newObjectMapper().writeValueAsBytes(parameters);
        if(bytes.length > (compressionThreshold != null && compressionThreshold.toBytes() > 0 ? compressionThreshold.toBytes() : 4 * 1024)) {
            action.compressedParameters = Utils.compress(bytes);
        } else {
            action.parameters = parameters;
        }
        action.parameterTypes = Stream.of(parameters)
                .map(p -> p != null ? p.getClass().getName() : null)
                .toArray(String[]::new);
        return action;
    }

    public String getMethod() {
        return method;
    }
    public Object[] getParameters() {
        return parameters;
    }
    public String[] getParameterTypes() {
        return parameterTypes;
    }
    public byte[] getCompressedParameters() {
        return compressedParameters;
    }
    public String getTime() {
        return time;
    }
    public String getVersion() {
        return VERSION;
    }
}
