package net.pincette.mongo;

import static net.pincette.rs.Util.asValueAsync;
import static net.pincette.rs.Util.emptyAsync;
import static net.pincette.util.Util.rethrow;

import com.mongodb.TransactionOptions;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * A <code>CompletionStage</code> wrapper around client sessions.
 *
 * @author Werner Donn\u00e9
 * @since 1.1
 */
public class Session {
  private Session() {}

  public static CompletionStage<Void> abortTransaction(final ClientSession session) {
    return emptyAsync(session.abortTransaction());
  }

  public static CompletionStage<Void> commitTransaction(final ClientSession session) {
    return emptyAsync(session.commitTransaction());
  }

  public static CompletionStage<ClientSession> create(final MongoClient client) {
    return asValueAsync(client.startSession());
  }

  /**
   * Runs a function in a transaction within a client session with the default transaction options.
   *
   * @param fn the function to run. When its completion stage completes to <code>null</code>, the
   *     transaction will be aborted. When it is not <code>null</code>, the transaction will be
   *     committed. When the function throws an exception, it will be rethrown after the abortion of
   *     the transaction.
   * @param session the client session.
   * @param <T> the result type.
   * @return The completion stage that returns success or failure.
   * @since 2.2.1
   */
  public static <T> CompletionStage<T> inTransaction(
      final Function<ClientSession, CompletionStage<T>> fn, final ClientSession session) {
    return inTransaction(fn, session, null);
  }

  /**
   * Runs a function in a transaction within a client session.
   *
   * @param fn the function to run. When its completion stage completes to <code>null</code>, the
   *     transaction will be aborted. When it is not <code>null</code>, the transaction will be
   *     committed. When the function throws an exception, it will be rethrown after the abortion of
   *     the transaction.
   * @param session the client session.
   * @param options the transaction options. It may be <code>null</code>, in which case the defaults
   *     are used.
   * @param <T> the result type.
   * @return The completion stage that returns success or failure.
   * @since 2.2.1
   */
  public static <T> CompletionStage<T> inTransaction(
      final Function<ClientSession, CompletionStage<T>> fn,
      final ClientSession session,
      final TransactionOptions options) {
    if (options != null) {
      session.startTransaction(options);
    } else {
      session.startTransaction();
    }

    return fn.apply(session)
        .thenComposeAsync(
            result ->
                result != null
                    ? commitTransaction(session).thenApply(r -> result)
                    : abortTransaction(session).thenApply(r -> null))
        .exceptionally(
            e -> {
              abortTransaction(session).toCompletableFuture().join();
              rethrow(e);
              return null;
            });
  }
}
