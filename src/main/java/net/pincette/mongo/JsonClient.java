package net.pincette.mongo;

import static com.mongodb.client.model.Filters.eq;
import static java.util.stream.Collectors.toList;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.toNative;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.BsonUtil.toDocument;
import static net.pincette.mongo.Collection.insertOne;
import static net.pincette.mongo.Collection.replaceOne;
import static net.pincette.rs.Chain.with;

import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

/**
 * These are convenience functions to use JSON with the MongoDB API.
 *
 * @author Werner Donn\u00e9
 * @since 1.4
 */
public class JsonClient {
  private static final String ID = "_id";

  private JsonClient() {}

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection, final List<? extends Bson> pipeline) {
    return aggregate(collection, pipeline, null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection, final JsonArray pipeline) {
    return aggregate(collection, fromJsonArray(pipeline), null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final List<? extends Bson> pipeline) {
    return aggregate(collection, session, pipeline, null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final JsonArray pipeline) {
    return aggregate(collection, session, fromJsonArray(pipeline), null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return Collection.aggregate(collection, pipeline, BsonDocument.class, setParameters)
        .thenApply(JsonClient::toJson);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final JsonArray pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregate(collection, fromJsonArray(pipeline), setParameters);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return Collection.aggregate(collection, session, pipeline, BsonDocument.class, setParameters)
        .thenApply(JsonClient::toJson);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> aggregate(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final JsonArray pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregate(collection, session, fromJsonArray(pipeline), setParameters);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection, final List<? extends Bson> pipeline) {
    return aggregationPublisher(collection, pipeline, null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection, final JsonArray pipeline) {
    return aggregationPublisher(collection, fromJsonArray(pipeline), null);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregationPublisher(
        () -> collection.aggregate(pipeline, BsonDocument.class), setParameters);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection,
      final JsonArray pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregationPublisher(collection, fromJsonArray(pipeline), setParameters);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregationPublisher(
        () -> collection.aggregate(session, pipeline, BsonDocument.class), setParameters);
  }

  /**
   * Finds JSON objects that come out of <code>pipeline</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param pipeline the given pipeline.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> aggregationPublisher(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final JsonArray pipeline,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return aggregationPublisher(collection, session, fromJsonArray(pipeline), setParameters);
  }

  private static Publisher<JsonObject> aggregationPublisher(
      final Supplier<AggregatePublisher<BsonDocument>> operation,
      final UnaryOperator<AggregatePublisher<BsonDocument>> setParameters) {
    return Optional.of(operation.get())
        .map(a -> setParameters != null ? setParameters.apply(a) : a)
        .map(JsonClient::toJson)
        .orElseGet(net.pincette.rs.Util::empty);
  }

  /**
   * Finds all JSON objects.
   *
   * @param collection the MongoDB collection.
   * @return The list of objects.
   * @since 2.0
   */
  public static CompletionStage<List<JsonObject>> find(final MongoCollection<Document> collection) {
    return find(collection, (Bson) null, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection, final Bson filter) {
    return find(collection, filter, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection, final JsonObject filter) {
    return find(collection, filter != null ? fromJson(filter) : null, null);
  }

  /**
   * Finds all JSON objects.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @return The list of objects.
   * @since 2.0
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection, final ClientSession session) {
    return find(collection, session, (Bson) null, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter. It may be <code>null</code>.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection, final ClientSession session, final Bson filter) {
    return find(collection, session, filter, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter. It may be <code>null</code>.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final JsonObject filter) {
    return find(collection, session, filter != null ? fromJson(filter) : null, null);
  }

  /**
   * Finds all JSON objects.
   *
   * @param collection the MongoDB collection.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 2.0
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return find(collection, (Bson) null, setParameters);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return (filter != null
            ? Collection.find(collection, filter, BsonDocument.class, setParameters)
            : Collection.find(collection, BsonDocument.class, setParameters))
        .thenApply(JsonClient::toJson);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final JsonObject filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return find(collection, filter != null ? fromJson(filter) : null, setParameters);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return (filter != null
            ? Collection.find(collection, session, filter, BsonDocument.class, setParameters)
            : Collection.find(collection, session, BsonDocument.class, setParameters))
        .thenApply(JsonClient::toJson);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The list of objects.
   * @since 1.4
   */
  public static CompletionStage<List<JsonObject>> find(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final JsonObject filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return find(collection, session, filter != null ? fromJson(filter) : null, setParameters);
  }

  /**
   * Finds all JSON objects.
   *
   * @param collection the MongoDB collection.
   * @return The object publisher.
   * @since 2.0
   */
  public static Publisher<JsonObject> findPublisher(final MongoCollection<Document> collection) {
    return findPublisher(collection, (Bson) null, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection, final Bson filter) {
    return findPublisher(collection, filter, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection, final JsonObject filter) {
    return findPublisher(collection, filter != null ? fromJson(filter) : null, null);
  }

  /**
   * Finds all JSON objects.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 2.0
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(collection, session, (Bson) null, setParameters);
  }

  /**
   * Finds all JSON objects.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @return The object publisher.
   * @since 2.0
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection, final ClientSession session) {
    return findPublisher(collection, session, (Bson) null, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter. It may be <code>null</code>.
   * @return The object publisher.
   * @since 2.0
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection, final ClientSession session, final Bson filter) {
    return findPublisher(collection, session, filter, null);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 2.0
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(collection, (Bson) null, setParameters);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(
        filter != null
            ? () -> collection.find(filter, BsonDocument.class)
            : () -> collection.find(BsonDocument.class),
        setParameters);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final JsonObject filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(collection, filter != null ? fromJson(filter) : null, setParameters);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final Bson filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(
        filter != null
            ? () -> collection.find(session, filter, BsonDocument.class)
            : () -> collection.find(session, BsonDocument.class),
        setParameters);
  }

  /**
   * Finds JSON objects that match <code>filter</code>.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter. It may be <code>null</code>.
   * @param setParameters a function to set the parameters for the result set.
   * @return The object publisher.
   * @since 1.4
   */
  public static Publisher<JsonObject> findPublisher(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final JsonObject filter,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return findPublisher(
        collection, session, filter != null ? fromJson(filter) : null, setParameters);
  }

  private static Publisher<JsonObject> findPublisher(
      final Supplier<FindPublisher<BsonDocument>> operation,
      final UnaryOperator<FindPublisher<BsonDocument>> setParameters) {
    return Optional.of(operation.get())
        .map(a -> setParameters != null ? setParameters.apply(a) : a)
        .map(JsonClient::toJson)
        .orElseGet(net.pincette.rs.Util::empty);
  }

  /**
   * Finds a JSON object. Only one should match the <code>filter</code>, otherwise the result will
   * be empty.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter.
   * @return The optional result.
   * @since 1.4
   */
  public static CompletionStage<Optional<JsonObject>> findOne(
      final MongoCollection<Document> collection, final Bson filter) {
    return Collection.findOne(collection, filter, BsonDocument.class, null)
        .thenApply(result -> result.map(BsonUtil::fromBson));
  }

  /**
   * Finds a JSON object. Only one should match the <code>filter</code>, otherwise the result will
   * be empty.
   *
   * @param collection the MongoDB collection.
   * @param filter the given filter.
   * @return The optional result.
   * @since 1.4
   */
  public static CompletionStage<Optional<JsonObject>> findOne(
      final MongoCollection<Document> collection, final JsonObject filter) {
    return findOne(collection, fromJson(filter));
  }

  /**
   * Finds a JSON object. Only one should match the <code>filter</code>, otherwise the result will
   * be empty.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter.
   * @return The optional result.
   * @since 1.4
   */
  public static CompletionStage<Optional<JsonObject>> findOne(
      final MongoCollection<Document> collection, final ClientSession session, final Bson filter) {
    return Collection.findOne(collection, session, filter, BsonDocument.class, null)
        .thenApply(result -> result.map(BsonUtil::fromBson));
  }

  /**
   * Finds a JSON object. Only one should match the <code>filter</code>, otherwise the result will
   * be empty.
   *
   * @param collection the MongoDB collection.
   * @param session the MongoDB session.
   * @param filter the given filter.
   * @return The optional result.
   * @since 1.4
   */
  public static CompletionStage<Optional<JsonObject>> findOne(
      final MongoCollection<Document> collection,
      final ClientSession session,
      final JsonObject filter) {
    return findOne(collection, session, fromJson(filter));
  }

  private static List<? extends Bson> fromJsonArray(final JsonArray array) {
    return array.stream().map(JsonValue::asJsonObject).map(BsonUtil::fromJson).collect(toList());
  }

  /**
   * Inserts the <code>collection</code> with <code>json</code>.
   *
   * @param collection the MongoDB collection.
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @return Whether the update was successful or not.
   * @since 1.4
   */
  public static CompletionStage<Boolean> insert(
      final MongoCollection<Document> collection, final JsonObject json) {
    return insert(collection, json, null);
  }

  /**
   * Inserts the <code>collection</code> with <code>json</code>.
   *
   * @param collection the MongoDB collection.
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.4
   */
  public static CompletionStage<Boolean> insert(
      final MongoCollection<Document> collection,
      final JsonObject json,
      final ClientSession session) {
    final Document document = toDocument(fromJson(json));

    return (session != null
            ? insertOne(collection, session, document)
            : insertOne(collection, document))
        .thenApply(InsertOneResult::wasAcknowledged);
  }

  private static List<JsonObject> toJson(final List<BsonDocument> list) {
    return list.stream().map(BsonUtil::fromBson).collect(toList());
  }

  private static Publisher<JsonObject> toJson(final Publisher<BsonDocument> pub) {
    return with(pub).map(BsonUtil::fromBson).get();
  }

  /**
   * Updates the <code>collection</code> with <code>json</code>. If the object doesn't exist yet it
   * is inserted.
   *
   * @param collection the MongoDB collection.
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param id the identifier for the object.
   * @return Whether the update was successful or not.
   * @since 1.4
   */
  public static CompletionStage<Boolean> update(
      final MongoCollection<Document> collection, final JsonObject json, final String id) {
    return update(collection, json, id, null);
  }

  /**
   * Updates the <code>collection</code> with <code>json</code>. If the object doesn't exist yet it
   * is inserted. The field <code>_id</code> in <code>json</code> is used as the key.
   *
   * @param collection the MongoDB collection.
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @return Whether the update was successful or not.
   * @since 1.4
   */
  public static CompletionStage<Boolean> update(
      final MongoCollection<Document> collection, final JsonObject json) {
    return update(collection, json, (ClientSession) null);
  }

  /**
   * Updates the <code>collection</code> with <code>json</code>. If the object doesn't exist yet it
   * is inserted.
   *
   * @param collection the MongoDB collection.
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param id the identifier for the object.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.4
   */
  public static CompletionStage<Boolean> update(
      final MongoCollection<Document> collection,
      final JsonObject json,
      final String id,
      final ClientSession session) {
    return update(collection, json, createValue(id), session);
  }

  /**
   * Updates the <code>collection</code> with <code>json</code>. If the object doesn't exist yet it
   * is inserted. The field <code>_id</code> in <code>json</code> is used as the key.
   *
   * @param collection the MongoDB collection.
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.4
   */
  public static CompletionStage<Boolean> update(
      final MongoCollection<Document> collection,
      final JsonObject json,
      final ClientSession session) {
    return update(collection, json, json.get(ID), session);
  }

  /**
   * Updates the <code>collection</code> with <code>json</code>. If the object doesn't exist yet it
   * is inserted.
   *
   * @param collection the MongoDB collection.
   * @param json the given JSON Event Sourcing object, which can be an aggregate, event or a
   *     command.
   * @param id the identifier for the object.
   * @param session the MongoDB session.
   * @return Whether the update was successful or not.
   * @since 1.4
   */
  public static CompletionStage<Boolean> update(
      final MongoCollection<Document> collection,
      final JsonObject json,
      final JsonValue id,
      final ClientSession session) {
    final Document document = toDocument(fromJson(json));
    final Bson filter = eq(ID, toNative(id));
    final ReplaceOptions options = new ReplaceOptions().upsert(true);

    return (session != null
            ? replaceOne(collection, session, filter, document, options)
            : replaceOne(collection, filter, document, options))
        .thenApply(UpdateResult::wasAcknowledged);
  }
}
