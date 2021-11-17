package replication;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.election.LeaderKey;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.context.ServletWebServerInitializedEvent;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

@Component
@Order(Ordered.LOWEST_PRECEDENCE - 2000)
@Conditional(Routing.class)
class Election2 implements ApplicationListener<ServletWebServerInitializedEvent>, DisposableBean {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Value("${spring.application.name:}")
    private String serviceName;

    @Value("${spring.etcd.lease-ttl-grant-in-seconds:100}")
    private long leaseTtlGrantInSeconds;
    @Value("${spring.etcd.lease-keep-alive-in-seconds:10}")
    private long leaseKeepAliveInSeconds;
    @Autowired
    private Client client;

    @Autowired
    private InetDiscoverer inetUtils;

    private LeaderKey leaderKey;
    private CompletableFuture<CampaignResponse> campaignFuture;
    private final Object lock = new Object();

    public void destroy() throws InterruptedException, ExecutionException, TimeoutException {
        LeaderKey localLeaderKey;
        synchronized (lock) {
            final CompletableFuture<CampaignResponse> localCampaignFuture = this.campaignFuture;
            if(localCampaignFuture != null) {
                localCampaignFuture.cancel(true);
            }
            localLeaderKey = this.leaderKey;
        }
        if(localLeaderKey != null && client.getElectionClient() != null) {
			Map<String, Object> resigningLog = Utils.map("leaderKey", localLeaderKey.getName().toString(StandardCharsets.UTF_8));
            log.info("[RESIGNING] " + Utils.toJson(resigningLog));
            final long timeout = Math.min(leaseTtlGrantInSeconds, leaseKeepAliveInSeconds) + 5;
            client.getElectionClient().resign(localLeaderKey).get(timeout, TimeUnit.SECONDS);
        }
    }

    @Override
    public void onApplicationEvent(ServletWebServerInitializedEvent event) {
        ByteSequence electionName = ByteSequence.from(serviceName.toUpperCase(), StandardCharsets.UTF_8);
        final CompletableFuture<LeaseGrantResponse> grantFuture = client.getLeaseClient().grant(leaseTtlGrantInSeconds);
        try {
            long leaseId = grantFuture.get().getID();
            client.getLeaseClient().keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
                @Override
                public void onNext(LeaseKeepAliveResponse value) {
                }
                @Override
                public void onError(Throwable t) {
                }
                @Override
                public void onCompleted() {
                }
            });
            log.info("KeepAlive lease:" + leaseId + "; Hex format:" + Long.toHexString(leaseId));

            int port = event.getWebServer().getPort();
            Map<String, String> proposal = Utils.map(
                    "hostInfo", String.format("http://%s:%s/", inetUtils.getLocalIp(), port)
            );
            String json = Utils.toJson(proposal);
            ByteSequence firstProposal = ByteSequence.from(json, StandardCharsets.UTF_8);
            Map<String, Object> campaigningLog = Utils.map(
                    "electionName", serviceName.toUpperCase(),
                    "leaseId", leaseId,
                    "proposal", proposal
            );
            log.info("[CAMPAIGNING] " + Utils.toJson(campaigningLog));
            synchronized (lock) {
                campaignFuture = client.getElectionClient().campaign(electionName, leaseId, firstProposal);
                campaignFuture
                        .thenAccept(leaderResponse -> {
                            log.info("[CAMPAIGNED] " + Utils.toJson(campaigningLog));
                            synchronized (lock) {
                                leaderKey = leaderResponse.getLeader();
                            }
                        })
                        .exceptionally(e -> {
                            log.error("", e);
                            return null;
                        });
            }
        } catch (InterruptedException e) {
            log.error("", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getMessage(), e.getCause());
        }
    }

}
