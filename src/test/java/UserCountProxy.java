import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import replication.ReplicableInstanceFactory;
import replication.ReplicationInstanceParameter;
import replication.Routing;

@Configuration
@Conditional(Routing.class)
public class UserCountProxy {

    private final Object lock = new Object();

    private final ReplicableInstanceFactory replicableInstanceFactory;

    public UserCountProxy(@Autowired ReplicableInstanceFactory replicableInstanceFactory) {
        this.replicableInstanceFactory = replicableInstanceFactory;
    }

    @Bean("replicableUserCount")
    @Primary
    UserCount replicableComplianceEngine(@Autowired UserCount delegate) {
        return proxy(delegate);
    }

    private <T> T proxy(T delegate) {
        final ReplicationInstanceParameter<T> parameter = ReplicationInstanceParameter.<T>newBuilder()
                .withDelegate(delegate)
                .withLock(lock)
                .build();
        return replicableInstanceFactory.newInstance(parameter);
    }
}
