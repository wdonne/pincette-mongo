package net.pincette.mongo;

import static javax.json.Json.createArrayBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
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

  static Optional<String> key(final JsonObject expression) {
    return Optional.of(expression.keySet())
        .filter(keys -> keys.size() == 1)
        .map(keys -> keys.iterator().next());
  }

  static JsonValue toArray(final Stream<JsonValue> values) {
    return values.reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1).build();
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
