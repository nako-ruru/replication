package replication;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.election.LeaderResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@Order(Ordered.LOWEST_PRECEDENCE - 3000)
@Conditional(Routing.class)
class LeaderDiscoverer implements ApplicationListener<ServletWebServerInitializedEvent> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Value("${spring.application.name}")
    private String serviceName;
    private int port;

    @Autowired
    private Client client;

    @Value("${application.rules.leader-discovery-timeout-in-seconds:30}")
    private long leaderDiscoveryTimeoutInSeconds;

    @Autowired
    private InetDiscoverer inetDiscoverer;

    private volatile URI leader;
    private final Object leaderLock = new Object();
 
    private boolean leaderDiscoveryEnabled;

    /**
     * Returns a pair instance
     * whose left value indicates whether the current running application is leader,
     * and right value indicates the current leader URI if the current running application is not leader, null otherwise.
     *
     * @return Returns a pair instance
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws TimeoutException
     */
    public Pair<Boolean, URI> isLeaderOrElse() throws InterruptedException, ExecutionException, TimeoutException {
        URI leaderLocalVar = getLeader();
        try {
            if(inetDiscoverer.getLocalIp().equals(leaderLocalVar.getHost()) && port == leaderLocalVar.getPort()) {
                return Pair.of(true, null);
            } else {
                return Pair.of(false, leaderLocalVar);
            }
        } catch (RuntimeException e) {
            throw e;
        }
    }

    @Override
    public void onApplicationEvent(ServletWebServerInitializedEvent event) {
        port = event.getWebServer().getPort();

        client.getElectionClient().observe(ByteSequence.from(serviceName.toUpperCase(), StandardCharsets.UTF_8), new Election.Listener() {
            @Override
            public void onNext(LeaderResponse response) {
                final String json = response.getKv().getValue().toString(StandardCharsets.UTF_8);
                final UriHolder uriHolder = Utils.fromJson(json, UriHolder.class);
                leader = URI.create(uriHolder.getHostInfo());
            }
            @Override
            public void onError(Throwable throwable) {
                logger.error("", throwable);
            }
            @Override
            public void onCompleted() {
            }
        });
    }

    public boolean isLeaderDiscoveryEnabled() {
        return leaderDiscoveryEnabled;
    }

    public void enableLeaderDiscovery() {
        this.leaderDiscoveryEnabled = true;
    }

    private URI getLeader() throws InterruptedException, ExecutionException, TimeoutException {
        if(leader == null) {
            synchronized (leaderLock) {
                if(leader == null) {
                    ByteSequence electionName = ByteSequence.from(serviceName.toUpperCase(), StandardCharsets.UTF_8);
                    final CompletableFuture<LeaderResponse> leaderResponseCompletableFuture = client.getElectionClient().leader(electionName);
                    LeaderResponse leaderResponse = leaderResponseCompletableFuture.get(leaderDiscoveryTimeoutInSeconds, TimeUnit.SECONDS);
                    final String s = leaderResponse.getKv().getValue().toString(StandardCharsets.UTF_8);
                    final UriHolder uriHolder = Utils.fromJson(s, UriHolder.class);
                    leader = URI.create(uriHolder.getHostInfo());
                }
            }
        }
        return leader;
    }
}
