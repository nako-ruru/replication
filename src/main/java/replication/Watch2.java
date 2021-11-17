package replication;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Order(Ordered.LOWEST_PRECEDENCE - 1000)
@Conditional(Routing.class)
class Watch2 implements ApplicationListener<ServletWebServerInitializedEvent> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Autowired
    private StreamUtils streamUtils;

    @Autowired
    private LeaderDiscoverer leaderDiscoverer;

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private ApplicationContext applicationContext;

    private final Map<String, Triple<Object, String, Method>> signatureToBeanAndMethod = new ConcurrentHashMap<>();

    @Override
    public void onApplicationEvent(ServletWebServerInitializedEvent event) {
        CountDownLatch replayingHistoryJobsCountDownLatch = new CountDownLatch(1);

        String key = streamUtils.getKey();
        {
            String group = streamUtils.getGroup();
            EventStreamUtils.createConsumerGroup(key, group, stringRedisTemplate);
            // 创建配置对象
            StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> streamMessageListenerContainerOptions = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                    .builder()
                    // 一次性最多拉取多少条消息
                    .batchSize(100)
                    // 执行消息轮询的执行器
                    .executor(this.threadPoolTaskExecutor)
                    // 消息消费异常的handler
                    .errorHandler(t -> log.error("", t))
                    // 超时时间，设置为0，表示不超时（超时后会抛出异常）
                    .pollTimeout(Duration.ofMillis(10))
                    // 序列化器
                    .serializer(new StringRedisSerializer())
                    .build();
            // 根据配置对象创建监听容器对象
            StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamMessageListenerContainer = StreamMessageListenerContainer
                    .create(this.redisConnectionFactory, streamMessageListenerContainerOptions);

            // 使用监听容器对象开始监听消费（使用的是手动确认方式）
            streamMessageListenerContainer.receive(
                    Consumer.from(group, "0"),
                    StreamOffset.create(key, ReadOffset.lastConsumed()),
                    this.new Listener(replayingHistoryJobsCountDownLatch)
            );
            // 启动监听
            streamMessageListenerContainer.start();
        }

        {
            try {
                replayingHistoryJobs(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            replayingHistoryJobsCountDownLatch.countDown();
        }
    }

    private void replayingHistoryJobs(String queue) throws JsonProcessingException, ExecutionException, InterruptedException, InvocationTargetException, IllegalAccessException {
        BlockingQueue<Future<Action2>> futures = new LinkedBlockingQueue<>();
        AtomicBoolean flag = new AtomicBoolean();
        new Thread(() -> {
            streamUtils.kvs(queue, r -> {
                final Map<String, String> map = r.getValue();
                /*
                Map<String, String> payload = Utils.map(
                        "payload", action.v,
                        "ttl", Long.toString(ttl),
                        "createTime", new SimpleDateFormat("yyyyMMdd HHmmss.SSS").format(new Date())
                );
                 */
                boolean process = true;
                try {
                    LocalDateTime createTime = LocalDateTime.parse(map.get("createTime"), DateTimeFormatter.ofPattern("yyyyMMdd HHmmss.SSS"));
                    long ttl = Long.parseLong(map.get("ttl"));
                    if(ttl > 0 && createTime.plusSeconds(ttl).isBefore(LocalDateTime.now())) {
                        process = false;
                    }
                } catch (Exception e) {
                    log.error("", e);
                }
                if(process) {
                    String value = map.get("payload");
                    final CompletableFuture<Action2> actionFuture = CompletableFuture.supplyAsync(() -> {
                        try {
                            return convert(value);
                        } catch (Exception e) {
                            log.error("", e);
                            return null;
                        }
                    });
                    futures.add(actionFuture);
                }
            });
            flag.set(true);
        }).start();

        Action2[] lastHolder = new Action2[1];
        Future<Action2> action2Future;
        while (!flag.get()) {
            while ((action2Future = futures.poll(5, TimeUnit.MILLISECONDS)) != null) {
                Action2 action2 = action2Future.get();
                if(action2 != null) {
                    action2.proceed();
                    lastHolder[0] = action2;
                }
            }
        }
        if(lastHolder[0] != null) {
            Action2 last = lastHolder[0];
            Map<String, Object> lastMessage = Utils.map(
                    "id", last.getId(),
                    "parameters", last.getParameters(),
                    "beanName", last.getBeanName(),
                    "method", Utils2.toReplicableString(last.getMethod())
            );
            final ObjectMapper mapper = newObjectMapper();
            String lastInfo = mapper.writeValueAsString(lastMessage);
            if(lastInfo.length() < 256) {
                log.info("last: {}", lastInfo);
            } else {
                Object[] simplifiedParameters = new String[last.getParameters().length];
                Arrays.fill(simplifiedParameters, "...");
                lastMessage.put("parameters", simplifiedParameters);
                String simplifiedLastInfo = mapper.writeValueAsString(lastMessage);
                log.info("last: {}", simplifiedLastInfo);
            }
        }

        leaderDiscoverer.enableLeaderDiscovery();
    }

    class Listener implements StreamListener<String, MapRecord<String, String, String>> {
        private final CountDownLatch replayHistoryJobsCountDownLatch;

        private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1,
                1L, TimeUnit.MINUTES,
                new LinkedBlockingQueue<>(),
                new CustomizableThreadFactory("watcher-deliver")
        );

        public Listener(CountDownLatch replayHistoryJobsCountDownLatch) {
            this.replayHistoryJobsCountDownLatch = replayHistoryJobsCountDownLatch;
            executor.allowCoreThreadTimeOut(true);
        }

        @Override
        public void onMessage(MapRecord<String, String, String> record) {
            try {
                String creator = record.getValue().get("creator");
                if(StringUtils.equalsIgnoreCase(creator, streamUtils.getGroup())) {
                    ack(record);
                    return;
                }
                replayHistoryJobsCountDownLatch.await();
                //以下if代码片段理论上不会发生，这里只是做了一写安全处理
                if (StringUtils.isBlank(creator)) {
                    if (leaderDiscoverer.isLeaderOrElse().getLeft()) {
                        ack(record);
                        return;
                    }
                }
                //handle heavy computing asynchronously
                final CompletableFuture<Pair<Action2, String>> actionFuture = CompletableFuture.supplyAsync(() -> {
                    final String value = getPayloadText(record);
                    try {
                        final Action2 action = Watch2.this.convert(value);
                        String message = null;
                        if (action != null) {
                            final Map<String, Object> map = Utils.map(
                                    "id", action.getId(),
                                    "bean", action.getBeanName(),
                                    "method", action.getMethod().getName(),
                                    "parameters", action.getParameters()
                            );
                            message = Utils.toJson(map);
                            if (action.getParameters() != null && action.getParameters().length > 0 && message.length() > 1024) {
                                map.put("parameters", "...");
                                message = Utils.toJson(map);
                            }
                        }
                        return Pair.of(action, message);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                Callable<?> callable = () -> {
                    Pair<Action2, String> pair = actionFuture.get();
                    if (pair != null && pair.getLeft() != null) {
                        pair.getLeft().proceed();
                        ack(record);
                        log.info("[WATCH] {}", pair.getRight());
                    } else {
                        log.warn("[WATCH] {}", Utils.toJson(convert(record)));
                    }
                    return null;
                };
                executor.submit(callable);
            } catch (InterruptedException e) {
                log.error("", e);
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("[WATCH] {}", Utils.toJson(convert(record)));
            }
        }

        private String getPayloadText(MapRecord<String, String, String> event) {
            return event.getValue().get("payload");
        }

        private Map<String, Object> convert(MapRecord<String, String, String> event) {
            return Utils.map(
                    "recordId", event.getId().getValue(),
                    "payload", Utils.abbreviate(getPayloadText(event), 256)
            );
        }

        private void ack(MapRecord<String, String, String> record) {
            stringRedisTemplate.opsForStream().acknowledge(streamUtils.getGroup(), record);
        }
    }

    private Action2 convert(String value) throws IOException {
        final ObjectMapper mapper = newObjectMapper();
        final Action jsonNode = mapper.readValue(value, Action.class);
        final String signature = jsonNode.getMethod();

        Triple<Object, String, Method> beanAndMethod = findBeanAndMethod(signature);

        if(beanAndMethod != null) {
            final byte[] compressedParameters = jsonNode.getCompressedParameters();
            Object[] parametersNode;
            if(ArrayUtils.isEmpty(compressedParameters)) {
                parametersNode = jsonNode.getParameters();
            } else if (ArrayUtils.isEmpty(jsonNode.getParameters())) {
                final byte[] decompress = Utils.decompress(compressedParameters, 0, compressedParameters.length);
                parametersNode = mapper.readValue(decompress, Object[].class);
            } else {
                parametersNode = jsonNode.getParameters();
                log.warn("either parameters or compressedParameters must be empty");
            }
            final String[] parameterTypesNode = jsonNode.getParameterTypes();
            final Class<?>[] actualParameterTypes = convertClasses(parameterTypesNode);
            final Type[] methodGenericParameterTypes = beanAndMethod.getRight().getGenericParameterTypes();
            Object[] args = IntStream.range(0, methodGenericParameterTypes.length)
                    .mapToObj(i -> {
                        try {
                            return convert(
                                    parametersNode[i],
                                    actualParameterTypes != null ? actualParameterTypes[i] : null,
                                    methodGenericParameterTypes[i]
                            );
                        } catch (RuntimeException e) {
                            throw e;
                        }
                    })
                    .toArray(Object[]::new);
            return new Action2(BigInteger.ZERO, args, beanAndMethod);
        }
        return null;
    }

    private Triple<Object, String, Method> findBeanAndMethod(String signature) {
        Triple<Object, String, Method> beanAndMethod = this.signatureToBeanAndMethod.get(signature);
        if(beanAndMethod == null) {
            this.signatureToBeanAndMethod.computeIfAbsent(signature, k -> {
                Multimap<String, Method> candidates = HashMultimap.create();
                for (String beanName : applicationContext.getBeanDefinitionNames()) {
                    final Object bean = applicationContext.getBean(beanName);
                    if (bean instanceof ReplicableHolder) {
                        Object delegate = bean;
                        while (AopUtils.isAopProxy(delegate)) {
                            delegate = AopProxyUtils.getSingletonTarget(delegate);
                        }
                        Collection<Method> methods = Utils2.getMethods(delegate.getClass());
                        for (Method method : methods) {
                            if (match(signature, method)) {
                                candidates.put(beanName, method);
                                if (candidates.size() >= 2) {
                                    throw new RuntimeException(String.format("duplicated replicable method signature: '%s'", signature));
                                }
                            }
                        }
                    }
                }
                if(candidates.isEmpty()) {
                    throw new RuntimeException(String.format("no replicable method signature: '%s'", signature));
                }
                final Map.Entry<String, Method> unique = candidates.entries().iterator().next();
                return Triple.of(applicationContext.getBean(unique.getKey()), unique.getKey(), unique.getValue());
            });
            beanAndMethod = this.signatureToBeanAndMethod.get(signature);
        }
        return beanAndMethod;
    }

    private static boolean match(String signature, Method method) {
        return method.isAnnotationPresent(Replicable.class) && signature.equals(Utils2.toReplicableString(method));
    }

    private static Object convert(Object fromValue, Class<?> actualParameterType, Type methodGenericParameterType) {
        final Type parameterType;
        if(actualParameterType == null || Collection.class.isAssignableFrom(actualParameterType) || Map.class.isAssignableFrom(actualParameterType)) {
            parameterType = methodGenericParameterType;
        } else {
            parameterType = actualParameterType;
        }
        final ObjectMapper mapper = newObjectMapper();
        return mapper.convertValue(fromValue, mapper.getTypeFactory().constructType(parameterType));
    }

    private static Class<?>[] convertClasses(String[] fromValue) {
        Class<?>[] actualParameterTypes = null;
        if(fromValue != null) {
            actualParameterTypes = Arrays.stream(fromValue)
                    .map(s -> {
                        if (s == null) {
                            return null;
                        }
                        try {
                            return Class.forName(s);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException("panic", e);
                        }
                    })
                    .toArray(Class[]::new);
        }
        return actualParameterTypes;
    }

    private static ObjectMapper newObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    }
}

class Action2 implements Comparable<Action2> {

    private final BigInteger id;
    private final Object[] parameters;
    private final Object bean;
    private final String beanName;
    private final Method method;

    public Action2(BigInteger id, Object[] parameters, Triple<Object, String, Method> beanAndMethod) {
        this.id = id;
        this.parameters = parameters;
        this.bean = beanAndMethod.getLeft();
        this.beanName = beanAndMethod.getMiddle();
        this.method = beanAndMethod.getRight();
    }

    public void proceed() throws InvocationTargetException, IllegalAccessException {
        if(!method.isAccessible()) {
            method.setAccessible(true);
        }
        method.invoke(bean, parameters);
    }

    public BigInteger getId() {
        return id;
    }

    public Object[] getParameters() {
        return parameters;
    }

    public Object getBean() {
        return bean;
    }

    public String getBeanName() {
        return beanName;
    }

    public Method getMethod() {
        return method;
    }

    @Override
    public int compareTo(Action2 o) {
        return this.id.compareTo(o.id);
    }
}

class EventStreamUtils {

    private static final Logger log = LoggerFactory.getLogger(EventStreamUtils.class);
    public static <K extends String> void createConsumerGroup(K key, String group, RedisTemplate<K, ?> redisTemplate) {
        try {
            // ReadOffset.from("0-0") will start reading stream from the very beginning.  Otherwise,
            // it will pick up at the point in the stream where the new group was created.
            //redisTemplate.opsForStream().createGroup(key, ReadOffset.from("0-0"), group);
            String command = Stream.of("XGROUP", "CREATE", key, group, "$", "MKSTREAM")
                    .map(s -> String.format("\"%s\"", s))
                    .collect(Collectors.joining(", ", "redis.call(", ")"));
            RedisScript<Void> script = RedisScript.of(command);
            redisTemplate.execute(script, Collections.emptyList());
        } catch (RedisSystemException e) {
            Throwable cause = e.getRootCause();
            if (cause != null && cause.getClass().getName().contains("RedisBusyException")) {
                log.info("STREAM - Redis group already exists, skipping Redis group creation: {}", group);
            } else {
                throw e;
            }
        }
    }
}