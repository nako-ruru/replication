# description
a java replication solution for master-slave architecture(using leader election) and event sourcing

# scenarios
1. stateful services which require multiple nodes
2. event-driven based services which require recovery on service restarting

# requirements
1. java 8 or higher
2. springframework 5, springframework.boot 2 and springframework.cloud 3
3. redis 5 or higher
4. etcd 3 or higher

# configuration sample
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
    compression-threshold:     # default 4 * 1024 bytes
    fetch-page-size: 10     # used of event sourcing, maximum number of events per loading
    fetch-print-size: 10    # used of event sourcing, a log information that indicates how many events have been loading already
```