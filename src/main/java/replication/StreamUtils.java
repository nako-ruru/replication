package replication;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.RedisZSetCommands;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Consumer;

@Component
@Conditional(Routing.class)
class StreamUtils {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Value("${spring.application.name:}")
    private String serviceName;

    @Value("${application.rules.fetch-page-size:2500}")
    private int limit;
    @Value("${application.rules.fetch-print-size:10000}")
    private int fetchPrintSize;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    private volatile String key;
    private volatile String group;

    private final Object lock = new Object();

    public void batchKvs(String key, Consumer<Collection<MapRecord<String, String, String>>> consumer) {
        long size = stringRedisTemplate.boundStreamOps(key).size();
        int loaded = 0;
        Range<String> range = Range.unbounded();
        for(;;) {
            List<MapRecord<String, String, String>> records = stringRedisTemplate.<String, String>boundStreamOps(key)
                    .range(range, RedisZSetCommands.Limit.limit().count(limit));
            if(records.isEmpty()) {
                break;
            }
            if(loaded / fetchPrintSize != (loaded += records.size()) / fetchPrintSize || loaded >= size && loaded % fetchPrintSize != 0) {
                log.info(String.format("total: %d, loaded: %d", size, loaded));
            }
            consumer.accept(records);
            //redis 5 doesn't support "("
            String value = records.get(records.size() - 1).getId().getValue();
            String[] elements = value.split("-");
            int seq = Integer.parseInt(elements[1]);
            String nextValue = String.format("%s-%s", elements[0], seq + 1);
            range = Range.rightUnbounded(Range.Bound.inclusive(nextValue));
        }
    }

    public void kvs(String key, Consumer<MapRecord<String, String, String>> consumer) {
        batchKvs(key, records -> {
            for (MapRecord<String, String, String> record : records) {
                consumer.accept(record);
            }
        });
    }

    public String getKey() {
        if(StringUtils.isBlank(key)) {
            synchronized (lock) {
                if(StringUtils.isBlank(key)) {
                    key = String.format("%s-events", serviceName);
                }
            }
        }
        return key;
    }

    public String getGroup() {
        if(StringUtils.isBlank(group)) {
            synchronized (lock) {
                if(StringUtils.isBlank(group)) {
                    group = getKey() + "-" + new SimpleDateFormat("yyyyMMdd-HHmmss-SSS").format(new Date());
                }
            }
        }
        return group;
    }

    @PreDestroy
    private void destroy() {
        if(StringUtils.isNoneBlank(getKey(), getGroup())) {
            stringRedisTemplate.opsForStream().destroyGroup(getKey(), getGroup());
        }
    }

}
