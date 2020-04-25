package net.pincette.mongo;

import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.mongo.Expression.applyFunctions;
import static net.pincette.mongo.Expression.arraysOperator;
import static net.pincette.mongo.Expression.arraysOperatorTwo;
import static net.pincette.mongo.Expression.functions;
import static net.pincette.mongo.Expression.isFalse;
import static net.pincette.mongo.Util.toArray;
import static net.pincette.util.Collections.intersection;
import static net.pincette.util.StreamUtil.slide;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.util.Collections;

class Sets {
  private Sets() {}

  static Function<JsonObject, JsonValue> allElementsTrue(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        createValue(
            applyFunctions(functions, json)
                .filter(values -> values.stream().noneMatch(Expression::isFalse))
                .isPresent());
  }

  static Function<JsonObject, JsonValue> anyElementsTrue(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        createValue(
            applyFunctions(functions, json)
                .filter(values -> values.stream().anyMatch(v -> !isFalse(v)))
                .isPresent());
  }

  private static boolean notIn(final JsonArray array, final JsonValue value) {
    return !array.contains(value);
  }

  static Function<JsonObject, JsonValue> setDifference(final JsonValue value) {
    return arraysOperatorTwo(value, Sets::setDifference);
  }

  private static JsonValue setDifference(final JsonArray a1, final JsonArray a2) {
    return toArray(a1.stream().filter(v -> notIn(a2, v)));
  }

  static Function<JsonObject, JsonValue> setEquals(final JsonValue value) {
    return arraysOperator(value, Sets::setEquals);
  }

  private static JsonValue setEquals(final List<JsonArray> values) {
    return createValue(
        values.isEmpty()
            || slide(values.stream().map(HashSet::new), 2)
                .map(pair -> pair.get(0).equals(pair.get(1)))
                .reduce((r1, r2) -> r1 && r2)
                .orElse(false));
  }

  static Function<JsonObject, JsonValue> setIntersection(final JsonValue value) {
    return arraysOperator(value, Sets::setIntersection);
  }

  private static JsonValue setIntersection(final List<JsonArray> values) {
    return setOp(values, Collections::intersection);
  }

  static Function<JsonObject, JsonValue> setIsSubset(final JsonValue value) {
    return arraysOperatorTwo(value, Sets::setIsSubset);
  }

  private static JsonValue setIsSubset(final JsonArray a1, final JsonArray a2) {
    return createValue(intersection(a1, a2).size() == a1.size());
  }

  private static JsonValue setOp(
      final List<JsonArray> values,
      final Function<Stream<java.util.Collection<JsonValue>>, Set<JsonValue>> op) {
    return toArray(op.apply(sets(values)).stream());
  }

  static Function<JsonObject, JsonValue> setUnion(final JsonValue value) {
    return arraysOperator(value, Sets::setUnion);
  }

  private static JsonValue setUnion(final List<JsonArray> values) {
    return setOp(values, Collections::union);
  }

  private static Stream<java.util.Collection<JsonValue>> sets(final List<JsonArray> values) {
    return values.stream().map(HashSet::new);
  }
}
