# Description
a java replication solution for master-slave architecture(using leader election) and event-sourcing architecture

# Scenarios
1. stateful services which require multiple nodes
2. event-driven based services which require recovery on service restarting

# Requirements
1. java 8 or higher
3. redis 5 or higher
4. etcd 3 or higher
2. test only with springframework 5, springframework.boot 2 and springframework.cloud 3

# Usage

## sample
> I have a service records numbers of online users.
> 
> Each time a user login, `UserCount.increment()` will be called;
> in other hand, each time a user logout, `UserCount.decrement()` will be called.
> 
> The service has a 'kick all' feature as well, in such case, `UserCount.reset()` will be called.

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;

@Component
class UserCount {

   private final AtomicLong counter = new AtomicLong();

   public void increment() {
      counter.incrementAndGet();
   }
   public void decrement() {
      counter.decrementAndGet();
   }
   public void reset() {
      counter.set(0);
   }
   public long get() {
      return counter.get();
   }
}

@RestController
class UserController {

   private final UserCount userCount;

   public UserController(@Autowired UserCount userCount) {
      this.userCount = userCount;
   }

   @PostMapping("/count/increments")
   public void increment() {
      userCount.increment();
   }
   @PostMapping("/count/decrements")
   public void decrement() {
      userCount.decrement();
   }
   @PostMapping("/count/resets")
   public void reset() {
      userCount.reset();
   }
   @GetMapping("/count")
   public long get() {
      return userCount.get();
   }
}

```
> Now I want to deploy such service with multiple nodes due to disaster tolerance.
> 
> As user count are recorded in memory, I have to copy data to other nodes.
> 
> Another way is that I can call same methods on other nodes. Luckily, this project exactly matches such requirement.

## alteration

Only 3 steps for you!
1. add @RouteToLeader to any controllers or their methods, better to controllers or methods of post, put and delete operations
2. add @Replicable to any components or their methods which need to fire replication events
3. create a proxy for events senders

```java
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import replication.Replicable;
import replication.RouteToLeader;

import java.util.concurrent.atomic.AtomicLong;

@Component
class UserCount {

   private final AtomicLong counter = new AtomicLong();

   @Replicable
   public void increment() {
      counter.incrementAndGet();
   }
   @Replicable
   public void decrement() {
      counter.decrementAndGet();
   }
   @Replicable(reset = true)
   public void reset() {
      counter.set(0);
   }
   public long get() {
      return counter.get();
   }
}

@RestController
@RouteToLeader
class UserController {

   private final UserCount userCount;

   public UserController(@Autowired UserCount userCount) {
      this.userCount = userCount;
   }

   @PostMapping("/count/increments")
   public void increment() {
      userCount.increment();
   }
   @PostMapping("/count/decrements")
   public void decrement() {
      userCount.decrement();
   }
   @PostMapping("/count/resets")
   public void reset() {
      userCount.reset();
   }
   @GetMapping("/count")
   @RouteToLeader(exclusive = true)
   public long get() {
      return userCount.get();
   }
}
```

```java
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
```

## configuration sample
``` yaml
spring:
  etcd:
    endpoints: http://192.168.221.197:2379, http://192.168.221.187:2379, http://192.168.221.186:2379
    lease-ttl-grant-in-seconds: 5
    lease-keep-alive-in-seconds: 5
    max-inbound-message-size: 10m
    core-pool-size: 8
    maximum-pool-size: 32
    local-ip:     # local-ip should be provided on condition of multiple non-loopback-addresses
  redis:
    database: 11
    host: 192.168.221.43
    port: 6379
    password:
    timeout: 120000

application:
  rules:
    # '1', 'yes', 'true' and 'on' all mean enabling routing writing operations to the leader
    # disable routing can help you focus on your business code
    enable-routing-to-leader-on-writing: 1
    leader-discovery-timeout-in-seconds: 30
    compression-threshold:     # if the events are too large, the events will be compressed, default 4 * 1024 bytes
    fetch-page-size: 10     # used for event sourcing, maximum number of events per loading
    fetch-print-size: 10    # used for event sourcing, a log information that indicates how many events have been loading already
```

# Diagnosis
1. What shall we do if the leader node fails send replication events to middlewares?
   > Check yaml configuration and redis configuration
2. What shall we do if the non-leader nodes don't receive replication events from middlewares?
   > Check yaml configuration and redis configuration, or just restart the nodes
3. What shall we do if no leader is found?
   > Check yaml configuration and etcd configuration

# Good luck
Please do not hesitate to contact me if you have any questions.