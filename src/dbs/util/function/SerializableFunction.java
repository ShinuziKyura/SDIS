package dbs.util.function;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunction<T> extends Function<Object[], T>, Serializable {

}
