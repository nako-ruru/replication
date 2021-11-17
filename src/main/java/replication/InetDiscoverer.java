package replication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.stereotype.Component;

@Component
public class InetDiscoverer {

    @Autowired
    private InetUtils inetUtils;
    @Value("${spring.etcd.local-ip:}")
    private String localIp;

    public String getLocalIp() {
        if(Utils.isNotBlank(localIp)) {
            return localIp;
        }
        return localIp = inetUtils.findFirstNonLoopbackAddress().getHostAddress();
    }
}
