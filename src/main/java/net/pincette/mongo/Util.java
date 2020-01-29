package net.pincette.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import net.pincette.rs.LambdaSubscriber;
import org.reactivestreams.Publisher;

/**
 * Some utilities.
 *
 * @author Werner Donn\u00e9
 */
class Util {
  private Util() {}

  private static <T> LambdaSubscriber<T> completer(final CompletableFuture<T> future) {
    return new LambdaSubscriber<>(future::complete, () -> {}, future::completeExceptionally);
  }

  private static <T> LambdaSubscriber<T> completerList(final CompletableFuture<List<T>> future) {
    final List<T> result = new ArrayList<>();

    return new LambdaSubscriber<>(
        result::add, () -> future.complete(result), future::completeExceptionally);
  }

  static <T> CompletionStage<T> wrap(final Supplier<Publisher<T>> publisher) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    publisher.get().subscribe(completer(future));

    return future;
  }

  static <T> CompletionStage<List<T>> wrapList(final Supplier<Publisher<T>> publisher) {
    final CompletableFuture<List<T>> future = new CompletableFuture<>();

    publisher.get().subscribe(completerList(future));

    return future;
  }
}
