package net.pincette.mongo;

import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.mongo.Expression.applyFunctions;
import static net.pincette.mongo.Expression.applyFunctionsNum;
import static net.pincette.mongo.Expression.functions;
import static net.pincette.mongo.Expression.isFalse;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;

class Booleans {
  private Booleans() {}

  static Function<JsonObject, JsonValue> and(final JsonValue value) {
    return combine(value, true, (r, v) -> r && !isFalse(v));
  }

  @SuppressWarnings("java:S4276") // For type inference.
  private static Function<JsonObject, JsonValue> combine(
      final JsonValue value,
      final boolean initial,
      final BiFunction<Boolean, JsonValue, Boolean> combiner) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json)
            .map(values -> values.stream().reduce(initial, combiner, (r1, r2) -> r1))
            .map(result -> result.booleanValue() ? TRUE : FALSE)
            .orElse(NULL);
  }

  private static JsonValue invert(final JsonValue value) {
    return isFalse(value) ? TRUE : FALSE;
  }

  static Function<JsonObject, JsonValue> or(final JsonValue value) {
    return combine(value, false, (r, v) -> r || !isFalse(v));
  }

  static Function<JsonObject, JsonValue> not(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 1)
            .map(values -> values.get(0))
            .map(Booleans::invert)
            .orElse(NULL);
  }
}
