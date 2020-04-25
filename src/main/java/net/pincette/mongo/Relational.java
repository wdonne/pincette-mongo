package net.pincette.mongo;

import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.mongo.Cmp.comparable;
import static net.pincette.mongo.Expression.applyFunctionsNum;
import static net.pincette.mongo.Expression.functions;

import java.util.List;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonValue;

/**
 * Implements relational MongoDB operators.
 *
 * @author Werner Donn\u00e9
 */
class Relational {
  private Relational() {}

  static Function<JsonValue, Function<JsonObject, JsonValue>> asFunction(
      final Function<JsonValue, Predicate<JsonObject>> predicate) {
    return value -> (json -> createValue(predicate.apply(value).test(json)));
  }

  private static Predicate<JsonObject> compare(final JsonValue value, final IntPredicate result) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .filter(values -> comparable(values.get(0), values.get(1)))
            .filter(values -> result.test(Cmp.compare(values.get(0), values.get(1))))
            .isPresent();
  }

  static Predicate<JsonObject> eq(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .filter(values -> values.get(0).equals(values.get(1)))
            .isPresent();
  }

  static Predicate<JsonObject> gt(final JsonValue value) {
    return compare(value, result -> result > 0);
  }

  static Predicate<JsonObject> gte(final JsonValue value) {
    return compare(value, result -> result >= 0);
  }

  static Predicate<JsonObject> lt(final JsonValue value) {
    return compare(value, result -> result < 0);
  }

  static Predicate<JsonObject> lte(final JsonValue value) {
    return compare(value, result -> result <= 0);
  }

  static Predicate<JsonObject> ne(final JsonValue value) {
    final Predicate<JsonObject> eq = eq(value);

    return json -> !eq.test(json);
  }
}
