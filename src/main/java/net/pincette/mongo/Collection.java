package net.pincette.mongo;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.CountOptions;
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
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.AggregatePublisher;
import com.mongodb.reactivestreams.client.DistinctPublisher;
import com.mongodb.reactivestreams.client.FindPublisher;
import com.mongodb.reactivestreams.client.MapReducePublisher;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.Success;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import net.pincette.rs.LambdaSubscriber;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

/**
 * These are <code>CompletionStage</code> wrappers around the MongoDB reactive streams client.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class Collection {
  private Collection() {}

  public static <D> CompletionStage<List<Document>> aggregate(
      final MongoCollection<D> collection,
      List<? extends Bson> pipeline,
      final UnaryOperator<AggregatePublisher<Document>> setParameters) {
    return execList(collection, c -> aggregatePub(c.aggregate(pipeline), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> aggregate(
      final MongoCollection<D> collection,
      List<? extends Bson> pipeline,
      final Class<T> resultClass,
      final UnaryOperator<AggregatePublisher<T>> setParameters) {
    return execList(
        collection, c -> aggregatePub(c.aggregate(pipeline, resultClass), setParameters));
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
      final List<? extends WriteModel<? extends D>> requests,
      final BulkWriteOptions options) {
    return exec(collection, c -> c.bulkWrite(requests, options));
  }

  private static <T> LambdaSubscriber<T> completer(final CompletableFuture<T> future) {
    return new LambdaSubscriber<>(future::complete, () -> {}, future::completeExceptionally);
  }

  private static <T> LambdaSubscriber<T> completerList(final CompletableFuture<List<T>> future) {
    final List<T> result = new ArrayList<>();

    return new LambdaSubscriber<>(
        result::add, () -> future.complete(result), future::completeExceptionally);
  }

  public static <D> CompletionStage<Long> countDocuments(final MongoCollection<D> collection) {
    return exec(collection, MongoCollection::countDocuments);
  }

  public static CompletionStage<Long> countDocuments(
      final MongoCollection<Document> collection, final Bson filter) {
    return exec(collection, c -> c.countDocuments(filter));
  }

  public static CompletionStage<Long> countDocuments(
      final MongoCollection<Document> collection, final Bson filter, final CountOptions options) {
    return exec(collection, c -> c.countDocuments(filter, options));
  }

  public static <D> CompletionStage<DeleteResult> deleteMany(
      final MongoCollection<D> collection, final Bson filter) {
    return exec(collection, c -> c.deleteMany(filter));
  }

  public static <D> CompletionStage<DeleteResult> deleteMany(
      final MongoCollection<D> collection, final Bson filter, final DeleteOptions options) {
    return exec(collection, c -> c.deleteMany(filter, options));
  }

  public static <D> CompletionStage<DeleteResult> deleteOne(
      final MongoCollection<D> collection, final Bson filter) {
    return exec(collection, c -> c.deleteOne(filter));
  }

  public static <D> CompletionStage<DeleteResult> deleteOne(
      final MongoCollection<D> collection, final Bson filter, final DeleteOptions options) {
    return exec(collection, c -> c.deleteOne(filter, options));
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
      final String fieldName,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<DistinctPublisher<T>> setParameters) {
    return execList(
        collection, c -> distinctPub(c.distinct(fieldName, filter, resultClass), setParameters));
  }

  private static <T> DistinctPublisher<T> distinctPub(
      final DistinctPublisher<T> pub, final UnaryOperator<DistinctPublisher<T>> setParameters) {
    return setParameters != null ? setParameters.apply(pub) : pub;
  }

  public static <D> CompletionStage<Long> estimatedDocumentCount(
      final MongoCollection<D> collection) {
    return exec(collection, MongoCollection::estimatedDocumentCount);
  }

  public static <D> CompletionStage<Long> estimatedDocumentCount(
      final MongoCollection<D> collection, final EstimatedDocumentCountOptions options) {
    return exec(collection, c -> c.estimatedDocumentCount(options));
  }

  private static <T, D> CompletionStage<T> exec(
      final MongoCollection<D> collection, final Function<MongoCollection<D>, Publisher<T>> op) {
    final CompletableFuture<T> future = new CompletableFuture<>();

    op.apply(collection).subscribe(completer(future));

    return future;
  }

  private static <T, D> CompletionStage<List<T>> execList(
      final MongoCollection<D> collection, final Function<MongoCollection<D>, Publisher<T>> op) {
    final CompletableFuture<List<T>> future = new CompletableFuture<>();

    op.apply(collection).subscribe(completerList(future));

    return future;
  }

  public static <D> CompletionStage<List<D>> find(
      final MongoCollection<D> collection, final UnaryOperator<FindPublisher<D>> setParameters) {
    return execList(collection, c -> findPub(c.find(), setParameters));
  }

  public static <D> CompletionStage<List<D>> find(
      final MongoCollection<D> collection,
      final Bson filter,
      final UnaryOperator<FindPublisher<D>> setParameters) {
    return execList(collection, c -> findPub(c.find(filter), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> find(
      final MongoCollection<D> collection,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return execList(collection, c -> findPub(c.find(resultClass), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> find(
      final MongoCollection<D> collection,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return execList(collection, c -> findPub(c.find(filter, resultClass), setParameters));
  }

  public static <T, D> CompletionStage<Optional<T>> findOne(
      final MongoCollection<D> collection,
      final Bson filter,
      final Class<T> resultClass,
      final UnaryOperator<FindPublisher<T>> setParameters) {
    return find(collection, filter, resultClass, setParameters)
        .thenApply(list -> Optional.of(list).filter(l -> l.size() == 1).map(l -> l.get(0)));
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
      final MongoCollection<D> collection,
      final Bson filter,
      final FindOneAndDeleteOptions options) {
    return exec(collection, c -> c.findOneAndDelete(filter, options));
  }

  public static <D> CompletionStage<D> findOneAndReplace(
      final MongoCollection<D> collection, final Bson filter, final D replacement) {
    return exec(collection, c -> c.findOneAndReplace(filter, replacement));
  }

  public static <D> CompletionStage<D> findOneAndReplace(
      final MongoCollection<D> collection,
      final Bson filter,
      final D replacement,
      final FindOneAndReplaceOptions options) {
    return exec(collection, c -> c.findOneAndReplace(filter, replacement, options));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection, final Bson filter, final Bson update) {
    return exec(collection, c -> c.findOneAndUpdate(filter, update));
  }

  public static <D> CompletionStage<D> findOneAndUpdate(
      final MongoCollection<D> collection,
      final Bson filter,
      final Bson update,
      final FindOneAndUpdateOptions options) {
    return exec(collection, c -> c.findOneAndUpdate(filter, update, options));
  }

  public static <D> CompletionStage<Success> insertMany(
      final MongoCollection<D> collection, final List<? extends D> documents) {
    return exec(collection, c -> c.insertMany(documents));
  }

  public static <D> CompletionStage<Success> insertMany(
      final MongoCollection<D> collection,
      final List<? extends D> documents,
      final InsertManyOptions options) {
    return exec(collection, c -> c.insertMany(documents, options));
  }

  public static <D> CompletionStage<Success> insertOne(
      final MongoCollection<D> collection, final D document) {
    return exec(collection, c -> c.insertOne(document));
  }

  public static <D> CompletionStage<Success> insertOne(
      final MongoCollection<D> collection, final D document, final InsertOneOptions options) {
    return exec(collection, c -> c.insertOne(document, options));
  }

  public static <D> CompletionStage<List<Document>> mapReduce(
      final MongoCollection<D> collection,
      final String mapFunction,
      final String reduceFunction,
      final UnaryOperator<MapReducePublisher<Document>> setParameters) {
    return execList(
        collection, c -> mapReducePub(c.mapReduce(mapFunction, reduceFunction), setParameters));
  }

  public static <T, D> CompletionStage<List<T>> mapReduce(
      final MongoCollection<D> collection,
      final String mapFunction,
      final String reduceFunction,
      final Class<T> resultClass,
      final UnaryOperator<MapReducePublisher<T>> setParameters) {
    return execList(
        collection,
        c -> mapReducePub(c.mapReduce(mapFunction, reduceFunction, resultClass), setParameters));
  }

  private static <T> MapReducePublisher<T> mapReducePub(
      final MapReducePublisher<T> pub, final UnaryOperator<MapReducePublisher<T>> setParameters) {
    return setParameters != null ? setParameters.apply(pub) : pub;
  }

  public static <D> CompletionStage<UpdateResult> replaceOne(
      final MongoCollection<D> collection, final Bson filter, final D replacement) {
    return exec(collection, c -> c.replaceOne(filter, replacement));
  }

  public static <D> CompletionStage<UpdateResult> replaceOne(
      final MongoCollection<D> collection,
      final Bson filter,
      final D replacement,
      final ReplaceOptions options) {
    return exec(collection, c -> c.replaceOne(filter, replacement, options));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection, final Bson filter, final Bson update) {
    return exec(collection, c -> c.updateMany(filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateMany(
      final MongoCollection<D> collection,
      final Bson filter,
      final Bson update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateMany(filter, update, options));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection, final Bson filter, final Bson update) {
    return exec(collection, c -> c.updateOne(filter, update));
  }

  public static <D> CompletionStage<UpdateResult> updateOne(
      final MongoCollection<D> collection,
      final Bson filter,
      final Bson update,
      final UpdateOptions options) {
    return exec(collection, c -> c.updateOne(filter, update, options));
  }
}
