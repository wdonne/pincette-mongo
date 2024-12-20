package net.pincette.mongo;

import static net.pincette.rs.Util.asListAsync;
import static net.pincette.rs.Util.asValueAsync;
import static org.reactivestreams.FlowAdapters.toFlowPublisher;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.EstimatedDocumentCountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.ClientSession;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

/**
 * These are <code>CompletionStage</code> wrappers around the MongoDB reactive streams client.
 *
 * @author Werner Donné
 * @since 1.0
 */
public class Collection {
  private Collection() {}

  public static <D> CompletionStage<List<D>> aggregate(
      final MongoCollection<D> collection,
      List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<D>> setParameters) {
    return execList(collection, c -> aggregatePub(c.aggregate(pipeline), setParameters));
  }

  public static <D> CompletionStage<List<D>> aggregate(
      final MongoCollection<D> collection,
      final ClientSession session,
      List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<D>> setParameters) {
    return execList(collection, c -> aggregatePub(c.aggregate(session, pipeline), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> aggregate(
      final MongoCollection<D> collection,
      List<? extends Bson> pipeline,
      final Class<T> resultClass,
      final UnaryOperator<AggregatePublisher<T>> setParameters) {
    return execList(
        collection, c -> aggregatePub(c.aggregate(pipeline, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> aggregate(
      final MongoCollection<D> collection,
      final ClientSession session,
      List<? extends Bson> pipeline,
      final Class<T> resultClass,
      final UnaryOperator<AggregatePublisher<T>> setParameters) {
    return execList(
        collection, c -> aggregatePub(c.aggregate(session, pipeline, resultClass), setParameters));
  }

  private static <T> AggregatePublisher<T> aggregatePub(
      final AggregatePublisher<T> pub, final UnaryOperator<AggregatePublisher<T>> setParameters) {
    return setParameters != null ? setParameters.apply(pub) : pub;
  }

  public static <D> CompletionStage<BulkWriteResult> bulkWrite(
      final MongoCollection<D> collection, final List<? extends WriteModel<? extends D>> requests) {
    return exec(collection, c -> c.bulkWrite(requests));
  }

  public static <D> CompletionStage<BulkWriteResult> bulkWrite(
      final MongoCollection<D> collection,
      final ClientSession session,
      final List<? extends WriteModel<? extends D>> requests) {
    return exec(collection, c -> c.bulkWrite(session, requests));
  }

  public static <D> CompletionStage<BulkWriteResult> bulkWrite(
      final MongoCollection<D> collection,
      final List<? extends WriteModel<? extends D>> requests,
      final BulkWriteOptions options) {
    return exec(collection, c -> c.bulkWrite(requests, options));
  }

  public static <D> CompletionStage<BulkWriteResult> bulkWrite(
      final MongoCollection<D> collection,
      final ClientSession session,
      final List<? extends WriteModel<? extends D>> requests,
      final BulkWriteOptions options) {
    return exec(collection, c -> c.bulkWrite(session, requests, options));
  }

  public static <D> CompletionStage<Long> countDocuments(final MongoCollection<D> collection) {
    return exec(collection, MongoCollection::countDocuments);
  }

  public static <D> CompletionStage<Long> countDocuments(
      final MongoCollection<D> collection, final ClientSession session) {
    return exec(collection, c -> c.countDocuments(session));
  }

  public static <D> CompletionStage<Long> countDocuments(
      final MongoCollection<D> collection, final Bson filter) {
    return exec(collection, c -> c.countDocuments(filter));
  }

  public static <D> CompletionStage<Long> countDocuments(
      final MongoCollection<D> collection, final ClientSession session, final Bson filter) {
    return exec(collection, c -> c.countDocuments(session, filter));
  }

  public static <D> CompletionStage<Long> countDocuments(
      final MongoCollection<D> collection, final Bson filter, final CountOptions options) {
    return exec(collection, c -> c.countDocuments(filter, options));
  }

  public static <D> CompletionStage<Long> countDocuments(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final CountOptions options) {
    return exec(collection, c -> c.countDocuments(session, filter, options));
  }

  public static CompletionStage<MongoCollection<Document>> create(
      final MongoDatabase database, final String name) {
    return execCreate(database, name, db -> db.createCollection(name));
  }

  public static CompletionStage<MongoCollection<Document>> create(
      final MongoDatabase database, final String name, final CreateCollectionOptions options) {
    return execCreate(database, name, db -> db.createCollection(name, options));
  }

  public static CompletionStage<MongoCollection<Document>> create(
      final MongoDatabase database,
      final ClientSession session,
      final String name,
      final CreateCollectionOptions options) {
    return execCreate(database, name, db -> db.createCollection(session, name, options));
  }

  public static <D> CompletionStage<DeleteResult> deleteMany(
      final MongoCollection<D> collection, final Bson filter) {
    return exec(collection, c -> c.deleteMany(filter));
  }

  public static <D> CompletionStage<DeleteResult> deleteMany(
      final MongoCollection<D> collection, final ClientSession session, final Bson filter) {
    return exec(collection, c -> c.deleteMany(session, filter));
  }

  public static <D> CompletionStage<DeleteResult> deleteMany(
      final MongoCollection<D> collection, final Bson filter, final DeleteOptions options) {
    return exec(collection, c -> c.deleteMany(filter, options));
  }

  public static <D> CompletionStage<DeleteResult> deleteMany(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final DeleteOptions options) {
    return exec(collection, c -> c.deleteMany(session, filter, options));
  }

  public static <D> CompletionStage<DeleteResult> deleteOne(
      final MongoCollection<D> collection, final Bson filter) {
    return exec(collection, c -> c.deleteOne(filter));
  }

  public static <D> CompletionStage<DeleteResult> deleteOne(
      final MongoCollection<D> collection, final ClientSession session, final Bson filter) {
    return exec(collection, c -> c.deleteOne(session, filter));
  }

  public static <D> CompletionStage<DeleteResult> deleteOne(
      final MongoCollection<D> collection, final Bson filter, final DeleteOptions options) {
    return exec(collection, c -> c.deleteOne(filter, options));
  }

  public static <D> CompletionStage<DeleteResult> deleteOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final DeleteOptions options) {
    return exec(collection, c -> c.deleteOne(session, filter, options));
  }

  public static <T, D> CompletionStage<List<T>> distinct(
      final MongoCollection<D> collection,
      final String fieldName,
      final Class<T> resultClass,
      final UnaryOperator<DistinctPublisher<T>> setParameters) {
    return execList(
        collection, c -> distinctPub(c.distinct(fieldName, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> distinct(
      final MongoCollection<D> collection,
      final ClientSession session,
      final String fieldName,
      final Class<T> resultClass,
      final UnaryOperator<DistinctPublisher<T>> setParameters) {
    return execList(
        collection, c -> distinctPub(c.distinct(session, fieldName, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> distinct(
      final MongoCollection<D> collection,
      final String fieldName,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<DistinctPublisher<T>> setParameters) {
    return execList(
        collection, c -> distinctPub(c.distinct(fieldName, filter, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> distinct(
      final MongoCollection<D> collection,
      final ClientSession session,
      final String fieldName,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<DistinctPublisher<T>> setParameters) {
    return execList(
        collection,
        c -> distinctPub(c.distinct(session, fieldName, filter, resultClass), setParameters));
  }

  private static <T> DistinctPublisher<T> distinctPub(
      final DistinctPublisher<T> pub, final UnaryOperator<DistinctPublisher<T>> setParameters) {
    return setParameters != null ? setParameters.apply(pub) : pub;
  }

  public static <D> CompletionStage<Void> drop(final MongoCollection<D> collection) {
    return exec(collection, MongoCollection::drop);
  }

  public static <D> CompletionStage<Long> estimatedDocumentCount(
      final MongoCollection<D> collection) {
    return exec(collection, MongoCollection::estimatedDocumentCount);
  }

  public static <D> CompletionStage<Long> estimatedDocumentCount(
      final MongoCollection<D> collection, final EstimatedDocumentCountOptions options) {
    return exec(collection, c -> c.estimatedDocumentCount(options));
  }

  /**
   * Runs a collection operation with only one result.
   *
   * @param collection the given collection.
   * @param op the collection operation.
   * @param <T> the result type.
   * @param <D> the document type.
   * @return The completion stage with the result.
   * @since 2.1
   */
  public static <T, D> CompletionStage<T> exec(
      final MongoCollection<D> collection, final Function<MongoCollection<D>, Publisher<T>> op) {
    return asValueAsync(toFlowPublisher(op.apply(collection)));
  }

  private static CompletionStage<MongoCollection<Document>> execCreate(
      final MongoDatabase database,
      final String name,
      final Function<MongoDatabase, Publisher<Void>> op) {
    return asValueAsync(toFlowPublisher(op.apply(database)))
        .thenApply(r -> database.getCollection(name));
  }

  /**
   * Runs a collection operation with more than one result.
   *
   * @param collection the given collection.
   * @param op the collection operation.
   * @param <T> the result type.
   * @param <D> the document type.
   * @return The completion stage with the result list.
   * @since 2.1
   */
  public static <T, D> CompletionStage<List<T>> execList(
      final MongoCollection<D> collection, final Function<MongoCollection<D>, Publisher<T>> op) {
    return asListAsync(toFlowPublisher(op.apply(collection)));
  }

  public static <D> CompletionStage<List<D>> find(
      final MongoCollection<D> collection, final UnaryOperator<FindPublisher<D>> setParameters) {
    return execList(collection, c -> findPub(c.find(), setParameters));
  }

  public static <D> CompletionStage<List<D>> find(
      final MongoCollection<D> collection,
      final ClientSession session,
      final UnaryOperator<FindPublisher<D>> setParameters) {
    return execList(collection, c -> findPub(c.find(session), setParameters));
  }

  public static <D> CompletionStage<List<D>> find(
      final MongoCollection<D> collection,
      final Bson filter,
      final UnaryOperator<FindPublisher<D>> setParameters) {
    return execList(collection, c -> findPub(c.find(filter), setParameters));
  }

  public static <D> CompletionStage<List<D>> find(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final UnaryOperator<FindPublisher<D>> setParameters) {
    return execList(collection, c -> findPub(c.find(session, filter), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> find(
      final MongoCollection<D> collection,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return execList(collection, c -> findPub(c.find(resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> find(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return execList(collection, c -> findPub(c.find(session, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> find(
      final MongoCollection<D> collection,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return execList(collection, c -> findPub(c.find(filter, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> find(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return execList(collection, c -> findPub(c.find(session, filter, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<Optional<T>> findOne(
      final MongoCollection<D> collection,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return find(collection, filter, resultClass, setParameters).thenApply(Collection::justOne);
  }

  public static <T, D> CompletionStage<Optional<T>> findOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return find(collection, session, filter, resultClass, setParameters)
        .thenApply(Collection::justOne);
  }

  private static <T> FindPublisher<T> findPub(
      final FindPublisher<T> pub, final UnaryOperator<FindPublisher<T>> setParameters) {
    return setParameters != null ? setParameters.apply(pub) : pub;
  }

  public static <D> CompletionStage<D> findOneAndDelete(
      final MongoCollection<D> collection, final Bson filter) {
    return exec(collection, c -> c.findOneAndDelete(filter));
  }

  public static <D> CompletionStage<D> findOneAndDelete(
      final MongoCollection<D> collection, final ClientSession session, final Bson filter) {
    return exec(collection, c -> c.findOneAndDelete(session, filter));
  }

  public static <D> CompletionStage<D> findOneAndDelete(
      final MongoCollection<D> collection,
      final Bson filter,
      final FindOneAndDeleteOptions options) {
    return exec(collection, c -> c.findOneAndDelete(filter, options));
  }

  public static <D> CompletionStage<D> findOneAndDelete(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final FindOneAndDeleteOptions options) {
    return exec(collection, c -> c.findOneAndDelete(session, filter, options));
  }

  public static <D> CompletionStage<D> findOneAndReplace(
      final MongoCollection<D> collection, final Bson filter, final D replacement) {
    return exec(collection, c -> c.findOneAndReplace(filter, replacement));
  }

  public static <D> CompletionStage<D> findOneAndReplace(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final D replacement) {
    return exec(collection, c -> c.findOneAndReplace(session, filter, replacement));
  }

  public static <D> CompletionStage<D> findOneAndReplace(
      final MongoCollection<D> collection,
      final Bson filter,
      final D replacement,
      final FindOneAndReplaceOptions options) {
    return exec(collection, c -> c.findOneAndReplace(filter, replacement, options));
  }

  public static <D> CompletionStage<D> findOneAndReplace(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final D replacement,
      final FindOneAndReplaceOptions options) {
    return exec(collection, c -> c.findOneAndReplace(session, filter, replacement, options));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection, final Bson filter, final Bson update) {
    return exec(collection, c -> c.findOneAndUpdate(filter, update));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Bson update) {
    return exec(collection, c -> c.findOneAndUpdate(session, filter, update));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection,
      final Bson filter,
      final Bson update,
      final FindOneAndUpdateOptions options) {
    return exec(collection, c -> c.findOneAndUpdate(filter, update, options));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Bson update,
      final FindOneAndUpdateOptions options) {
    return exec(collection, c -> c.findOneAndUpdate(session, filter, update, options));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection, final Bson filter, final List<? extends Bson> update) {
    return exec(collection, c -> c.findOneAndUpdate(filter, update));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final List<? extends Bson> update) {
    return exec(collection, c -> c.findOneAndUpdate(session, filter, update));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection,
      final Bson filter,
      final List<? extends Bson> update,
      final FindOneAndUpdateOptions options) {
    return exec(collection, c -> c.findOneAndUpdate(filter, update, options));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final List<? extends Bson> update,
      final FindOneAndUpdateOptions options) {
    return exec(collection, c -> c.findOneAndUpdate(session, filter, update, options));
  }

  public static <D> CompletionStage<InsertManyResult> insertMany(
      final MongoCollection<D> collection, final List<? extends D> documents) {
    return exec(collection, c -> c.insertMany(documents));
  }

  public static <D> CompletionStage<InsertManyResult> insertMany(
      final MongoCollection<D> collection,
      final ClientSession session,
      final List<? extends D> documents) {
    return exec(collection, c -> c.insertMany(session, documents));
  }

  public static <D> CompletionStage<InsertManyResult> insertMany(
      final MongoCollection<D> collection,
      final List<? extends D> documents,
      final InsertManyOptions options) {
    return exec(collection, c -> c.insertMany(documents, options));
  }

  public static <D> CompletionStage<InsertManyResult> insertMany(
      final MongoCollection<D> collection,
      final ClientSession session,
      final List<? extends D> documents,
      final InsertManyOptions options) {
    return exec(collection, c -> c.insertMany(session, documents, options));
  }

  public static <D> CompletionStage<InsertOneResult> insertOne(
      final MongoCollection<D> collection, final D document) {
    return exec(collection, c -> c.insertOne(document));
  }

  public static <D> CompletionStage<InsertOneResult> insertOne(
      final MongoCollection<D> collection, final ClientSession session, final D document) {
    return exec(collection, c -> c.insertOne(session, document));
  }

  public static <D> CompletionStage<InsertOneResult> insertOne(
      final MongoCollection<D> collection, final D document, final InsertOneOptions options) {
    return exec(collection, c -> c.insertOne(document, options));
  }

  public static <D> CompletionStage<InsertOneResult> insertOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final D document,
      final InsertOneOptions options) {
    return exec(collection, c -> c.insertOne(session, document, options));
  }

  private static <T> Optional<T> justOne(final List<T> list) {
    return Optional.of(list).filter(l -> l.size() == 1).map(l -> l.get(0));
  }

  public static <D> CompletionStage<UpdateResult> replaceOne(
      final MongoCollection<D> collection, final Bson filter, final D replacement) {
    return exec(collection, c -> c.replaceOne(filter, replacement));
  }

  public static <D> CompletionStage<UpdateResult> replaceOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final D replacement) {
    return exec(collection, c -> c.replaceOne(session, filter, replacement));
  }

  public static <D> CompletionStage<UpdateResult> replaceOne(
      final MongoCollection<D> collection,
      final Bson filter,
      final D replacement,
      final ReplaceOptions options) {
    return exec(collection, c -> c.replaceOne(filter, replacement, options));
  }

  public static <D> CompletionStage<UpdateResult> replaceOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final D replacement,
      final ReplaceOptions options) {
    return exec(collection, c -> c.replaceOne(session, filter, replacement, options));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection, final Bson filter, final Bson update) {
    return exec(collection, c -> c.updateMany(filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Bson update) {
    return exec(collection, c -> c.updateMany(session, filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection,
      final Bson filter,
      final Bson update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateMany(filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Bson update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateMany(session, filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection, final Bson filter, final List<? extends Bson> update) {
    return exec(collection, c -> c.updateMany(filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final List<? extends Bson> update) {
    return exec(collection, c -> c.updateMany(session, filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection,
      final Bson filter,
      final List<? extends Bson> update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateMany(filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final List<? extends Bson> update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateMany(session, filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection, final Bson filter, final Bson update) {
    return exec(collection, c -> c.updateOne(filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Bson update) {
    return exec(collection, c -> c.updateOne(session, filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection,
      final Bson filter,
      final Bson update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateOne(filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final Bson update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateOne(session, filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection, final Bson filter, final List<? extends Bson> update) {
    return exec(collection, c -> c.updateOne(filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final List<? extends Bson> update) {
    return exec(collection, c -> c.updateOne(session, filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection,
      final Bson filter,
      final List<? extends Bson> update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateOne(filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection,
      final ClientSession session,
      final Bson filter,
      final List<? extends Bson> update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateOne(session, filter, update, options));
  }
}
