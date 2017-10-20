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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.protocol.RedisCommand;

import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.FutureResult;
import org.springframework.lang.Nullable;

/**
 * Lettuce specific {@link FutureResult} implementation. <br />
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
@SuppressWarnings("rawtypes")
class LettuceResult<T, R> extends FutureResult<RedisCommand<?, T, ?>> {

	private final boolean convertPipelineAndTxResults;

	LettuceResult(Future<T> resultHolder) {
		this(resultHolder, false, val -> val);
	}

	LettuceResult(Future<T> resultHolder, boolean convertPipelineAndTxResults, @Nullable Converter<T, ?> converter) {
		this(resultHolder, () -> null, convertPipelineAndTxResults, converter);
	}

	@SuppressWarnings("unchecked")
	LettuceResult(Future<T> resultHolder, Supplier<R> defaultReturnValue, boolean convertPipelineAndTxResults,
			@Nullable Converter<T, ?> converter) {

		super((RedisCommand) resultHolder, converter, defaultReturnValue);
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#get()
	 */
	@Nullable
	@Override
	@SuppressWarnings("unchecked")
	public T get() {
		return (T) getResultHolder().getOutput().get();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.FutureResult#seeksConversion()
	 */
	@Override
	public boolean seeksConversion() {
		return convertPipelineAndTxResults;
	}

	/**
	 * Lettuce specific {@link FutureResult} implementation of a throw away status result.
	 */
	static class LettuceStatusResult extends LettuceResult<Object, Object> {

		@SuppressWarnings("unchecked")
		LettuceStatusResult(Future<?> resultHolder) {
			super((Future) resultHolder);
			setStatus(true);
		}
	}

	/**
	 * Lettuce specific {@link FutureResult} implementation of a transaction result.
	 */
	static class LettuceTxResult<T, R> extends FutureResult<T> {

		private final boolean convertPipelineAndTxResults;

		LettuceTxResult(T resultHolder) {
			this(resultHolder, false, val -> (R) val);
		}

		LettuceTxResult(T resultHolder, boolean convertPipelineAndTxResults, Converter<T, R> converter) {
			this(resultHolder, () -> null, convertPipelineAndTxResults, converter);
		}

		LettuceTxResult(T resultHolder, Supplier<Object> defaultReturnValue, boolean convertPipelineAndTxResults,
				Converter<T, R> converter) {
			super(resultHolder, converter, defaultReturnValue);
			this.convertPipelineAndTxResults = convertPipelineAndTxResults;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object get() {
			return getResultHolder();
		}

		@Override
		public boolean seeksConversion() {
			return convertPipelineAndTxResults;
		}
	}

	/**
	 * Lettuce specific {@link FutureResult} implementation of a throw away status result.
	 */
	static class LettuceTxStatusResult extends LettuceTxResult<Object, Object> {

		LettuceTxStatusResult(Object resultHolder) {
			super(resultHolder);
			setStatus(true);
		}
	}

	/**
	 * Builder for constructing {@link LettuceResult}.
	 *
	 * @param <T>
	 * @param <R>
	 * @since 2.1
	 */
	static class LettuceResultBuilder<T, R> {

		private final Object response;
		private Converter<T, R> converter;
		private boolean convertPipelineAndTxResults = false;
		private Supplier<?> nullValueDefault = () -> null;

		LettuceResultBuilder(Object response) {

			this.response = response;
			this.converter = (source) -> (R) source;
		}

		static <T> LettuceResultBuilder<T, Object> forResponse(Future<T> response) {
			return new LettuceResultBuilder<>(response);
		}

		static <T> LettuceResultBuilder<T, Object> forResponse(T response) {
			return new LettuceResultBuilder<>(response);
		}

		LettuceResultBuilder<T, R> mappedWith(Converter<T, R> converter) {

			this.converter = converter;
			return (LettuceResultBuilder<T, R>) this;
		}

		<R> LettuceResultBuilder<T, R> defaultNullTo(R value) {
			return (defaultNullTo(() -> value));
		}

		<R> LettuceResultBuilder<T, R> defaultNullTo(Supplier<R> value) {

			this.nullValueDefault = value;
			return (LettuceResultBuilder<T, R>) this;
		}

		LettuceResultBuilder<T, R> convertPipelineAndTxResults(boolean flag) {

			convertPipelineAndTxResults = flag;
			return this;
		}

		LettuceResult<T, R> build() {
			return new LettuceResult((Future<T>) response, nullValueDefault, convertPipelineAndTxResults, converter);
		}

		LettuceTxResult<T, R> buildTxResult() {
			return new LettuceTxResult(response, nullValueDefault, convertPipelineAndTxResults, converter);
		}

		LettuceResult buildStatusResult() {
			return new LettuceStatusResult((Future<T>) response);
		}
	}
}
