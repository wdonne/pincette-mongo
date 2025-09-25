package net.pincette.mongo;

import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.mongo.Cmp.comparable;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.implementations;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.IntPredicate;
import javax.json.JsonObject;
import javax.json.JsonValue;

/**
 * Implements relational MongoDB operators.
 *
 * @author Werner Donné
 */
class Relational {
  private Relational() {}

  static Operator asFunction(final BiFunction<JsonValue, Features, RelOp> predicate) {
    return (value, features) ->
        ((json, vars) -> createValue(predicate.apply(value, features).test(json, vars)));
  }

  private static RelOp compare(
      final JsonValue value, final IntPredicate result, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .filter(values -> comparable(values.getFirst(), values.get(1)))
            .filter(values -> result.test(Cmp.compare(values.getFirst(), values.get(1))))
            .isPresent();
  }

  static RelOp eq(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .filter(values -> values.getFirst().equals(values.get(1)))
            .isPresent();
  }

  static RelOp gt(final JsonValue value, final Features features) {
    return compare(value, result -> result > 0, features);
  }

  static RelOp gte(final JsonValue value, final Features features) {
    return compare(value, result -> result >= 0, features);
  }

  static RelOp lt(final JsonValue value, final Features features) {
    return compare(value, result -> result < 0, features);
  }

  static RelOp lte(final JsonValue value, final Features features) {
    return compare(value, result -> result <= 0, features);
  }

  static RelOp ne(final JsonValue value, final Features features) {
    final BiPredicate<JsonObject, Map<String, JsonValue>> eq = eq(value, features);

    return (json, vars) -> !eq.test(json, vars);
  }

  interface RelOp extends BiPredicate<JsonObject, Map<String, JsonValue>> {}
}
