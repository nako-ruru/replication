package replication;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.stream.Stream;

public class Routing implements Condition {

    private Routing() {
    }

    private static final String K = "application.rules.enable-routing-to-leader-on-writing";
    public static final String ENABLE_ROUTING_TO_LEADER_ON_WRITING = "${" + K + "}";

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String testValue = context.getEnvironment().getProperty(K);
        return Utils.isNotBlank(testValue) && Stream.of("1", "yes", "true", "on").anyMatch(testValue::equalsIgnoreCase);

    }
}
