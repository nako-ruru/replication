package replication;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.StringJoiner;
import java.util.stream.Stream;

class Utils2 {

    private Utils2() {
        throw new Error("死心吧，你得不到我！");
    }

    static String toReplicableString(Method m) {
        Replicable replicable = m.getAnnotation(Replicable.class);
        if (Utils.isNotBlank(replicable.value())) {
            return replicable.value();
        }
        return toString(m);
    }

    private static String toString(Method m) {
        StringBuilder sb = new StringBuilder();
        sb.append(m.getDeclaringClass().getTypeName()).append('#');
        sb.append(m.getName());
        sb.append('(');
        StringJoiner sj = new StringJoiner(",");
        for (Class<?> parameterType : m.getParameterTypes()) {
            sj.add(parameterType.getTypeName());
        }
        sb.append(sj);
        sb.append(')');
        return sb.toString();
    }

    static Collection<Method> getMethods(Class<?> clazz) {
        Collection<Method> methods = new LinkedList<>(Arrays.asList(clazz.getDeclaredMethods()));
        Class<?> class0 = clazz;
        while (class0 != Object.class) {
            class0 = class0.getSuperclass();
            Stream.of(class0.getDeclaredMethods())
                    .filter(m -> !Modifier.isPrivate(m.getModifiers()) && !Modifier.isStatic(m.getModifiers()) && !Modifier.isAbstract(m.getModifiers()))
                    .forEach(methods::add);
        }
        return methods;
    }
}
