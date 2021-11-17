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