package rmi;

import java.io.Serializable;

import util.function.SerializableFunction;

public class RemoteFunction<T> implements Serializable {
	private SerializableFunction<T> operation;
	private Object[] operators;

	public RemoteFunction(SerializableFunction<T> operation) {
		this.operation = operation;
		this.operators = new Object[0];
	}

	public RemoteFunction(SerializableFunction<T> operation, Object[] operators) {
		this.operation = operation;
		this.operators = operators;
	}

	public T call() {
		return operation.apply(operators);
	}
}
