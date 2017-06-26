package com.ibm.async_util;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Combinators {
	private Combinators() {
	}

	@SafeVarargs
	private static <T> CompletionStage<Collection<T>> all(final CompletableFuture<T>... futures) {
		return CompletableFuture.allOf(futures).thenApply(ignored -> {
			return Stream.of(futures).map(CompletableFuture::join).collect(Collectors.toList());
		});
	}

	@SuppressWarnings("unchecked")
	public static <T> CompletionStage<Collection<T>> all(final Collection<CompletionStage<T>> futures) {
		return all(futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new));
	}

	@SuppressWarnings("unchecked")
	public static <T, A, R> CompletionStage<R> collect(
			final Collection<CompletionStage<T>> futures, 
			final Collector<T, A, R> collector) {
		@SuppressWarnings("rawtypes")
		CompletableFuture[] arr = futures.stream().map(CompletionStage::toCompletableFuture).toArray(CompletableFuture[]::new);
		return CompletableFuture.allOf(arr).thenApply(ignored -> {
			return Stream.of((CompletableFuture<T>[]) arr)
					.map(CompletableFuture::join)
					.collect(collector);
		});
		
	}
}
