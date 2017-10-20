/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.Response;

import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.lang.Nullable;

/**
 * Jedis specific {@link FutureResult} implementation. <br />
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <T> The data type of the object that holds the future result (usually of type Future).
 * @param <R> The data type of the result type.
 * @since 2.1
 */
class JedisResult<T, R> extends FutureResult<Response<?>> {

	private final boolean convertPipelineAndTxResults;

	JedisResult(Response<T> resultHolder) {
		this(resultHolder, false, null);
	}

	JedisResult(Response<T> resultHolder, boolean convertPipelineAndTxResults, @Nullable Converter<T, ?> converter) {
		this(resultHolder, () -> null, convertPipelineAndTxResults, converter);
	}

	JedisResult(Response<T> resultHolder, Supplier<R> defaultReturnValue, boolean convertPipelineAndTxResults,
			@Nullable Converter<T, ?> converter) {

		super(resultHolder, converter, defaultReturnValue);
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#get()
	 * @return
	 */
	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public T get() {
		return (T) getResultHolder().get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#seeksConversion()
	 */
	public boolean seeksConversion() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Jedis specific {@link FutureResult} implementation of a throw away status result.
	 */
	static class JedisStatusResult extends JedisResult<Object, Object> {

		@SuppressWarnings("unchecked")
		<T> JedisStatusResult(Response<T> resultHolder, Converter<T, ?> converter) {

			super((Response) resultHolder, false, (Converter) converter);
			setStatus(true);
		}
	}

	/**
	 * Builder for constructing {@link JedisResult}.
	 *
	 * @param <T>
	 * @param <R>
	 * @since 2.1
	 */
	static class JedisResultBuilder<T, R> {

		private final Response<T> response;
		private Converter<T, ?> converter;
		private boolean convertPipelineAndTxResults = false;
		private Supplier<?> nullValueDefault = () -> null;

		JedisResultBuilder(Response<T> response) {

			this.response = response;
			this.converter = (source) -> source;
		}

		static <T> JedisResultBuilder<T, ?> forResponse(Response<T> response) {
			return new JedisResultBuilder<>(response);
		}

		<R> JedisResultBuilder<T, R> mappedWith(Converter<T, R> converter) {

			this.converter = converter;
			return (JedisResultBuilder<T, R>) this;
		}

		<R> JedisResultBuilder<T, R> defaultNullTo(R value) {
			return (defaultNullTo(() -> value));
		}

		<R> JedisResultBuilder<T, R> defaultNullTo(Supplier<R> value) {

			this.nullValueDefault = value;
			return (JedisResultBuilder<T, R>) this;
		}

		JedisResultBuilder<T, R> convertPipelineAndTxResults(boolean flag) {

			convertPipelineAndTxResults = flag;
			return this;
		}

		@SuppressWarnings("unchecked")
		JedisResult<T, R> build() {
			return new JedisResult(response, nullValueDefault, convertPipelineAndTxResults, converter);
		}

		JedisStatusResult buildStatusResult() {
			return new JedisStatusResult(response, converter);
		}
	}
}
