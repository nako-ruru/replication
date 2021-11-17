package replication;

import com.google.common.primitives.Ints;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.unit.DataSize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@Conditional(Routing.class)
class EtcdConfigurer implements WebMvcConfigurer {

    @Bean
    @ConditionalOnMissingBean
    Client client(
            @Value("${spring.etcd.endpoints:}")String[] endpoints,
            @Value("${spring.etcd.core-pool-size:}")String corePoolSizeText,
            @Value("${spring.etcd.maximum-pool-size:}")String maximumPoolSizeText,
            @Value("${spring.etcd.max-inbound-message-size:}") String maxInboundMessageSizeText
    ) throws ScriptException {
        ClientBuilder clientBuilder = Client.builder()
                .endpoints(endpoints);
        if(Utils.isNotBlank(maxInboundMessageSizeText)) {
            int maxInboundMessageSize;
            Pattern pattern = Pattern.compile("\\s*(\\d+[kKmM])B?\\s*");
            final Matcher matcher = pattern.matcher(maxInboundMessageSizeText);
            if(matcher.matches()) {
                maxInboundMessageSize = Ints.checkedCast(DataSize.parse(matcher.group(1).toUpperCase() + "B").toBytes());
            } else {
                ScriptEngineManager manager = new ScriptEngineManager();
                ScriptEngine engine = manager.getEngineByName("js");
                Number result = (Number) engine.eval(maxInboundMessageSizeText);
                maxInboundMessageSize = result.intValue();
            }
            clientBuilder = clientBuilder.maxInboundMessageSize(maxInboundMessageSize);
        }
        if(Utils.isNotBlank(corePoolSizeText) || Utils.isNotBlank(maximumPoolSizeText)) {
            int corePoolSize = Utils.isNotBlank(corePoolSizeText) ? Integer.parseInt(corePoolSizeText.trim()) : 0;
            int maximumPoolSize = Utils.isNotBlank(maximumPoolSizeText) ? Integer.parseInt(maximumPoolSizeText.trim()) : corePoolSize;
            ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
                    corePoolSize,
                    maximumPoolSize,
                    1,
                    TimeUnit.MINUTES,
                    new LinkedBlockingQueue<>()
            );
            threadPoolExecutor.allowCoreThreadTimeOut(true);
            clientBuilder = clientBuilder.executorService(threadPoolExecutor);
        }
        return clientBuilder
                .build();
    }

    @Bean
    FollowerInterceptor followerInterceptor() {
        return new FollowerInterceptor();
    }

    @Bean
    Watch2 watch() {
        return new Watch2();
    }

    @Bean
    @ConditionalOnMissingBean
    ThreadPoolTaskExecutor threadPoolTaskExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.initialize();
        return threadPoolTaskExecutor;
    }

    @Bean
    @ConditionalOnMissingBean
    RestTemplate restTemplate() {
        return new RestTemplate();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(followerInterceptor());
    }

    public static boolean hasMethod(HandlerMethod handlerMethod, RequestMethod method) {
        Collection<RequestMethod> requestMethods = Stream.of(GetMapping.class, PostMapping.class, PutMapping.class, DeleteMapping.class, PatchMapping.class)
                .filter(handlerMethod::hasMethodAnnotation)
                .flatMap(mapping -> Stream.of(mapping.getAnnotation(RequestMapping.class).method()))
                .collect(Collectors.toSet());
        if(handlerMethod.hasMethodAnnotation(RequestMapping.class)) {
            final RequestMethod[] methods = handlerMethod.getMethodAnnotation(RequestMapping.class).method();
            if(methods.length == 0) {
                return true;
            }
            requestMethods.addAll(Arrays.asList(methods));
        }
        return requestMethods.contains(method);
    }

}
