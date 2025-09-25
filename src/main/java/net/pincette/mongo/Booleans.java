package net.pincette.mongo;

import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.mongo.Expression.applyImplementations;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.implementations;
import static net.pincette.mongo.Expression.isFalse;

import java.util.List;
import java.util.function.BiFunction;
import javax.json.JsonValue;

class Booleans {
  private Booleans() {}

  static Implementation and(final JsonValue value, final Features features) {
    return combine(value, true, (r, v) -> r && !isFalse(v), features);
  }

  @SuppressWarnings("java:S4276") // For type inference.
  private static Implementation combine(
      final JsonValue value,
      final boolean initial,
      final BiFunction<Boolean, JsonValue, Boolean> combiner,
      final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(implementations, json, vars)
            .map(values -> values.stream().reduce(initial, combiner, (r1, r2) -> r1))
            .map(result -> result ? TRUE : FALSE)
            .orElse(NULL);
  }

  private static JsonValue invert(final JsonValue value) {
    return isFalse(value) ? TRUE : FALSE;
  }

  static Implementation or(final JsonValue value, final Features features) {
    return combine(value, false, (r, v) -> r || !isFalse(v), features);
  }

  static Implementation not(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 1)
            .map(List::getFirst)
            .map(Booleans::invert)
            .orElse(NULL);
  }
}
