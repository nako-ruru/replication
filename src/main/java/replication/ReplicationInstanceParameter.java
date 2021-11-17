package replication;

import java.lang.reflect.Field;

public class ReplicationInstanceParameter<T> {

    private ReplicationInstanceParameter() {
    }

    private T delegate;
    private Object lock;

    public T getDelegate() {
        return delegate;
    }

    public Object getLock() {
        return lock;
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static final class Builder<T> {
        private T delegate;
        private Object lock;

        private Builder() {
        }

        public Builder<T> withDelegate(T delegate) {
            this.delegate = delegate;
            return this;
        }

        public Builder<T> withLock(Object lock) {
            this.lock = lock;
            return this;
        }

        public ReplicationInstanceParameter<T> build() {
            try {
                ReplicationInstanceParameter<T> that = new ReplicationInstanceParameter<>();
                for (Field thisField : Builder.class.getDeclaredFields()) {
                    if (!thisField.isAccessible()) {
                        thisField.setAccessible(true);
                    }
                    final Field thatField = ReplicationInstanceParameter.class.getDeclaredField(thisField.getName());
                    if(!thatField.isAccessible()) {
                        thatField.setAccessible(true);
                    }
                    thatField.set(that, thisField.get(this));
                }
                return that;
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new Error("我去……", e);
            }
        }

    }
}
