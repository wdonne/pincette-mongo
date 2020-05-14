package net.pincette.mongo;

import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.mongo.Cmp.comparable;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.implementations;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntPredicate;
import javax.json.JsonObject;
import javax.json.JsonValue;

/**
 * Implements relational MongoDB operators.
 *
 * @author Werner Donn\u00e9
 */
class Relational {
  private Relational() {}

  static Operator asFunction(final Function<JsonValue, RelOp> predicate) {
    return value -> ((json, vars) -> createValue(predicate.apply(value).test(json, vars)));
  }

  private static RelOp compare(final JsonValue value, final IntPredicate result) {
    final List<Implementation> implementations = implementations(value);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .filter(values -> comparable(values.get(0), values.get(1)))
            .filter(values -> result.test(Cmp.compare(values.get(0), values.get(1))))
            .isPresent();
  }

  static RelOp eq(final JsonValue value) {
    final List<Implementation> implementations = implementations(value);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .filter(values -> values.get(0).equals(values.get(1)))
            .isPresent();
  }

  static RelOp gt(final JsonValue value) {
    return compare(value, result -> result > 0);
  }

  static RelOp gte(final JsonValue value) {
    return compare(value, result -> result >= 0);
  }

  static RelOp lt(final JsonValue value) {
    return compare(value, result -> result < 0);
  }

  static RelOp lte(final JsonValue value) {
    return compare(value, result -> result <= 0);
  }

  static RelOp ne(final JsonValue value) {
    final BiPredicate<JsonObject, Map<String, JsonValue>> eq = eq(value);

    return (json, vars) -> !eq.test(json, vars);
  }

  interface RelOp extends BiPredicate<JsonObject, Map<String, JsonValue>> {}
}
