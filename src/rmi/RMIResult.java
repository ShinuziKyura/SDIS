package rmi;

import java.io.Serializable;

import util.function.SerializableFunction;

public class RMIResult<T> implements Serializable {
	private SerializableFunction<T> operation;
	private Object[] operators;

	public RMIResult (SerializableFunction<T> operation) {
		this.operation = operation;
	}

	public RMIResult (SerializableFunction<T> operation, Object[] operators) {
		this.operation = operation;
		this.operators = operators;
	}

	public T call() {
		return operation.apply(operators);
	}
}
