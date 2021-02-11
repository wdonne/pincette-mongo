package net.pincette.mongo;

import static com.mongodb.client.model.Filters.eq;
import static java.util.UUID.randomUUID;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.JsonClient.findOne;
import static net.pincette.mongo.JsonClient.update;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.Reducer.forEach;
import static net.pincette.rs.Reducer.forEachJoin;
import static net.pincette.util.Util.must;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.reactivestreams.client.MongoCollection;
import java.util.Optional;
import javax.json.JsonObject;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestUpdate {
  static final String APP = "pincette-mongo-test";
  private static final String COLLECTION = "pincette-mongo-test";
  private static final String ID = "_id";
  private static Resources resources;

  @AfterAll
  public static void after() {
    cleanUpCollections();
    resources.close();
  }

  @BeforeAll
  public static void before() {
    resources = new Resources();
    cleanUpCollections();
  }

  private static void cleanUpCollections() {
    forEachJoin(
        with(resources.database.listCollectionNames()).filter(name -> name.startsWith(APP)).get(),
        name -> forEach(resources.database.getCollection(name).drop(), v -> {}));
  }

  protected void drop(final String collection) {
    forEachJoin(resources.database.getCollection(collection).drop(), v -> {});
  }

  @Test
  @DisplayName("Diff update 1")
  void diffUpdate1() {
    run(o(f("test", a(v(0), v(1), v(2), v(3), v(4)))), o(f("test", a(v(0), v(1)))));
  }

  @Test
  @DisplayName("Diff update 2")
  void diffUpdate2() {
    run(o(f("test", a(v(0), v(1)))), o(f("test", a(v(0), v(1), v(2), v(3), v(4)))));
  }

  @Test
  @DisplayName("Diff update 3")
  void diffUpdate3() {
    run(o(f("test", o(f("test1", v(0))))), o(f("test", o(f("test1", v(0))))));
  }

  @Test
  @DisplayName("Diff update 4")
  void diffUpdate4() {
    run(o(f("test", o(f("test1", v(0))))), o(f("test", o(f("test2", v(0))))));
  }

  @Test
  @DisplayName("Diff update 5")
  void diffUpdate5() {
    run(o(f("test", o(f("test1", v(0))))), o(f("test", o(f("test1", v(1))))));
  }

  @Test
  @DisplayName("Diff update 6")
  void diffUpdate6() {
    run(o(f("test", o(f("test1", v(0))))), o(f("test", o(f("test2", v(1)), f("test3", v(4))))));
  }

  @Test
  @DisplayName("Diff update 7")
  void diffUpdate7() {
    run(o(f("test", o(f("test2", v(1)), f("test3", v(4))))), o(f("test", o(f("test1", v(0))))));
  }

  @Test
  @DisplayName("Diff update 8")
  void diffUpdate8() {
    run(
        o(f("test", o(f("test2", v(1)), f("test3", v(4))))),
        o(f("test", o(f("test2", v(1)), f("test4", v(3))))));
  }

  @Test
  @DisplayName("Diff update 9")
  void diffUpdate9() {
    run(
        o(f("test", a(v(0), v(1), v(2), v(3), v(4)))),
        o(f("test", a(v(0), a(v(1), v(2), a(v(0)))))));
  }

  @Test
  @DisplayName("Diff update 10")
  void diffUpdate10() {
    run(
        o(f("test", a(v(0), v(1), a(v(2), v(3), v(4))))),
        o(f("test", a(v(0), v(1), a(v(2), v(3), v(5))))));
  }

  @Test
  @DisplayName("Diff update 11")
  void diffUpdate11() {
    run(
        o(f("test", a(v(0), v(1), a(v(2), v(3), v(4))))),
        o(f("test", a(v(0), v(1), a(v(2), v(3))))));
  }

  @Test
  @DisplayName("Diff update 12")
  void diffUpdate12() {
    run(
        o(f("test", a(v(0), v(1), a(v(2), v(3), v(4))))),
        o(f("test", a(v(0), a(v(0), v(9)), v(1), a(v(2), v(3))))));
  }

  @Test
  @DisplayName("Diff update 13")
  void diffUpdate13() {
    run(
        o(f("test", a(v(0), a(v(0), v(9)), v(1), a(v(2), v(3))))),
        o(f("test", a(v(0), v(1), a(v(2), v(3), v(4))))));
  }

  private void prepare(final JsonObject message) {
    drop(COLLECTION);

    update(resources.database.getCollection(COLLECTION), message)
        .thenApply(result -> must(result, r -> r))
        .toCompletableFuture()
        .join();
  }

  private void run(final JsonObject source, final JsonObject target) {
    final String id = randomUUID().toString();
    final JsonObject s = createObjectBuilder(source).add(ID, id).build();
    final JsonObject t = createObjectBuilder(target).add(ID, id).build();

    prepare(s);

    final MongoCollection<Document> collection = resources.database.getCollection(COLLECTION);

    assertEquals(
        t,
        update(collection, s, t)
            .thenApply(result -> must(result, r -> r))
            .thenComposeAsync(result -> findOne(collection, eq(ID, fromJson(s.get(ID)))))
            .thenApply(result -> must(result, Optional::isPresent))
            .thenApply(Optional::get)
            .toCompletableFuture()
            .join());
  }
}
