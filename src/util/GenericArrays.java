package util;

import java.lang.reflect.Array;

public class GenericArrays {
	// Code adapted from here: https://stackoverflow.com/a/80503
	public static <T> T join(T first, T second) throws IllegalArgumentException {
		if (!first.getClass().isArray() || !second.getClass().isArray()) {
			throw new IllegalArgumentException();
		}

		Class<?> result_type;
		Class<?> first_type = first.getClass().getComponentType();
		Class<?> second_type = second.getClass().getComponentType();

		if (first_type.isAssignableFrom(second_type)) {
			result_type = first_type;
		}
		else if (second_type.isAssignableFrom(first_type)) {
			result_type = second_type;
		}
		else {
			throw new IllegalArgumentException();
		}

		int first_length = java.lang.reflect.Array.getLength(first);
		int second_length = java.lang.reflect.Array.getLength(second);

		@SuppressWarnings("unchecked")
		T result = (T) java.lang.reflect.Array.newInstance(result_type, first_length + second_length);
		System.arraycopy(first, 0, result, 0, first_length);
		System.arraycopy(second, 0, result, first_length, second_length);

		return result;
	}

	// Code adapted from here: https://stackoverflow.com/a/80503
	public static <T> T[] split(T source, int position) throws IllegalArgumentException {
		if (!source.getClass().isArray() || position < 0 || position > java.lang.reflect.Array.getLength(source)) {
			throw new IllegalArgumentException();
		}

		Class<?> result_type = source.getClass();

		int result_first_length = position;
		int result_second_length = java.lang.reflect.Array.getLength(source) - position;

		@SuppressWarnings("unchecked")
		T result_first = (T) java.lang.reflect.Array.newInstance(result_type.getComponentType(), result_first_length);
		@SuppressWarnings("unchecked")
		T result_second = (T) java.lang.reflect.Array.newInstance(result_type.getComponentType(), result_second_length);

		System.arraycopy(source, 0, result_first, 0, result_first_length);
		System.arraycopy(source, result_first_length, result_second, 0, result_second_length);

		@SuppressWarnings("unchecked")
		T[] result = (T[]) java.lang.reflect.Array.newInstance(result_type, 2);

		result[0] = result_first;
		result[1] = result_second;

		return result;
	}

}
