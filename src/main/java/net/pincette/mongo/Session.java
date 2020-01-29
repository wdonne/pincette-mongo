package net.pincette.mongo;

import static net.pincette.mongo.Util.wrap;

import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.MongoClient;
import java.util.concurrent.CompletionStage;

/**
 * A <code>CompletionStage</code> wrapper around client sessions.
 *
 * @author Werner Donn\u00e9
 * @since 1.1
 */
public class Session {
  private Session() {}

  public static CompletionStage<Void> abortTransaction(final ClientSession session) {
    return wrap(session::abortTransaction);
  }

  public static CompletionStage<Void> commitTransaction(final ClientSession session) {
    return wrap(session::commitTransaction);
  }

  public static CompletionStage<ClientSession> create(final MongoClient client) {
    return wrap(client::startSession);
  }
}
