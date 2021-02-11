package net.pincette.mongo;

import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static javax.json.JsonPatch.Operation.fromOperationName;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createPatch;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.toDotSeparated;
import static net.pincette.mongo.JsonClient.fromJsonStream;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.takeWhile;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.getParent;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonPatch;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;
import org.bson.conversions.Bson;

/**
 * Support for MongoDB updates.
 *
 * @author Werner Donn\u00e9
 * @since 2.1
 */
public class Patch {
  private static final String EACH = "$each";
  private static final String FROM = "from";
  private static final String OP = "op";
  private static final String PATH = "path";
  private static final String POSITION = "$position";
  private static final String PUSH = "$push";
  private static final String SET = "$set";
  private static final String UNSET = "$unset";
  private static final String VALUE = "value";

  private Patch() {}

  private static JsonObject add(final JsonObject original, final JsonObject op) {
    return add(original, op, op.get(VALUE));
  }

  private static JsonObject add(
      final JsonObject original, final JsonObject op, final JsonValue value) {
    final String path = op.getString(PATH);

    return arrayValue(original, path)
        .map(a -> setInArray(a, value))
        .orElseGet(() -> set(path, value));
  }

  private static Optional<ArrayValue> arrayValue(final JsonObject original, final String path) {
    final String parent = getParent(path, "/");

    return getLastSegment(path, "/")
        .filter(net.pincette.util.Util::isInteger)
        .map(Integer::parseInt)
        .flatMap(
            position -> getArray(original, parent).map(a -> new ArrayValue(parent, position, a)));
  }

  private static JsonObject copy(final JsonObject original, final JsonObject op) {
    return add(original, op, getValue(original, op.getString(FROM)).orElse(NULL));
  }

  private static Stream<Pair<JsonObject, JsonObject>> incremental(
      final JsonObject original, final Stream<JsonObject> patch) {
    return takeWhile(
        patch,
        op -> pair(original, op),
        (pair, op) -> pair(incremental(pair.first, pair.second), op),
        pair -> true);
  }

  private static JsonObject incremental(final JsonObject original, final JsonObject op) {
    return createPatch(a(op)).apply(original).asJsonObject();
  }

  private static Stream<JsonObject> move(final JsonObject original, final JsonObject op) {
    return of(remove(original, op, FROM), copy(original, op));
  }

  private static Stream<JsonObject> objects(final Stream<JsonValue> values) {
    return values.filter(JsonUtil::isObject).map(JsonValue::asJsonObject);
  }

  private static Stream<JsonObject> operation(final JsonObject original, final JsonObject op) {
    switch (fromOperationName(op.getString(OP))) {
      case ADD:
        return of(add(original, op));
      case COPY:
        return of(copy(original, op));
      case MOVE:
        return move(original, op);
      case REMOVE:
        return of(remove(original, op, PATH));
      case REPLACE:
        return of(replace(op));
      default:
        return empty();
    }
  }

  private static JsonObject remove(
      final JsonObject original, final JsonObject op, final String field) {
    final String path = op.getString(field);

    return arrayValue(original, path)
        .map(a -> set(a.path, removeFromArray(a)))
        .orElseGet(() -> unset(path));
  }

  private static JsonValue removeFromArray(final ArrayValue arrayValue) {
    return JsonUtil.remove(arrayValue.array, arrayValue.position);
  }

  private static JsonObject replace(final JsonObject op) {
    return set(op.getString(PATH), op.get(VALUE));
  }

  private static JsonObject set(final String path, final JsonValue value) {
    return o(f(SET, o(f(toDotSeparated(path), value))));
  }

  private static JsonObject setInArray(final ArrayValue arrayValue, final JsonValue value) {
    return o(
        f(
            PUSH,
            o(
                f(
                    toDotSeparated(arrayValue.path),
                    o(f(EACH, a(value)), f(POSITION, v(arrayValue.position)))))));
  }

  private static JsonObject unset(final String path) {
    return o(f(UNSET, o(f(toDotSeparated(path), v("")))));
  }

  /**
   * Creates a list of update operators from a JSON patch, with which a MongoDB document can be
   * updated using a bulk write. Root paths are not supported. You can use a plain update for that.
   *
   * @param original the JSON object that will be updated.
   * @param patch the JSON patch.
   * @return The generated aggregation pipeline.
   * @since 2.1
   */
  public static List<Bson> updateOperators(final JsonObject original, final JsonPatch patch) {
    return fromJsonStream(updateOperators(original, objects(patch.toJsonArray().stream())));
  }

  /**
   * Creates an array of update operators from a JSON patch, with which a MongoDB document can be
   * updated using a bulk write. Root paths are not supported. You can use a plain update for that.
   *
   * @param original the JSON object that will be updated.
   * @param patch the JSON patch.
   * @return The generated aggregation pipeline.
   * @since 2.1
   */
  public static JsonArray updateOperators(final JsonObject original, final JsonArray patch) {
    return updateOperators(original, objects(patch.stream()))
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  /**
   * Creates a stream of update operators from a JSON patch, with which a MongoDB document can be
   * updated using a bulk write. Root paths are not supported. You can use a plain update for that.
   *
   * @param original the JSON object that will be updated.
   * @param patch the JSON patch.
   * @return The generated aggregation pipeline.
   * @since 2.1
   */
  public static Stream<JsonObject> updateOperators(
      final JsonObject original, final Stream<JsonObject> patch) {
    return incremental(original, patch.filter(JsonUtil::isObject).map(JsonValue::asJsonObject))
        .flatMap(pair -> operation(pair.first, pair.second));
  }

  private static class ArrayValue {
    private final JsonArray array;
    private final String path;
    private final int position;

    private ArrayValue(final String path, final int position, final JsonArray array) {
      this.path = path;
      this.position = position;
      this.array = array;
    }
  }
}
