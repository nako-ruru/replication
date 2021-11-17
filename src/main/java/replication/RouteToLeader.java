package replication;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation that indicates the http call will be routed to the leader.
 * Normally, an http call with writing operation such as post, put and delete should be routed to the leader, because
 * in this way can we ensure the order of executing to be unique.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RouteToLeader {
    boolean exclusive() default false;
}
