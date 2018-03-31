package dbs.rmi;

import java.io.Serializable;

import dbs.util.function.SerializableFunction;

public class RemoteFunction<T> implements Serializable {
	private SerializableFunction<T> operation;
	private Object[] operators;

	public RemoteFunction(SerializableFunction<T> operation) {
		this.operation = operation;
	}

	public RemoteFunction(SerializableFunction<T> operation, Object[] operators) {
		this.operation = operation;
		this.operators = operators;
	}

	public T call() {
		return operation.apply(operators);
	}
}
