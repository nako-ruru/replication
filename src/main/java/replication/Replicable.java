package replication;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Replicable {
    String value() default "";
    /**
     * ttl in seconds
     * @return ttl in seconds
     */
    long ttl() default -1;
    String ttlExpression() default "";
    boolean reset() default false;
    boolean exclusive() default false;
}

