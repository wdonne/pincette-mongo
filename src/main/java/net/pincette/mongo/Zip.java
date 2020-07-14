package net.pincette.mongo;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.max;
import static java.lang.Math.min;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.generate;
import static javax.json.JsonValue.NULL;
import static net.pincette.mongo.Expression.applyImplementations;
import static net.pincette.mongo.Expression.isFalse;
import static net.pincette.mongo.Expression.member;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.mongo.Expression.memberFunctions;
import static net.pincette.mongo.Util.toArray;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;
import net.pincette.util.StreamUtil;

/**
 * Implements the MongoDB <code>$zip</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Zip {
  private static final String DEFAULTS = "defaults";
  private static final String INPUTS = "inputs";
  private static final String USE_LONGEST_LENGTH = "useLongestLength";

  private Zip() {}

  private static List<JsonValue> defaults(final JsonValue defaults, final int end) {
    return Optional.of(defaults)
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .map(defs -> concat(defs.stream(), defaultsNull(max(end - defs.size(), 0))))
        .orElseGet(() -> defaultsNull(end))
        .collect(toList());
  }

  private static Stream<JsonValue> defaultsNull(final int size) {
    return StreamUtil.zip(rangeExclusive(0, size), generate(() -> NULL)).map(pair -> pair.second);
  }

  private static Pair<Integer, Integer> shortestLongest(final List<JsonValue> values) {
    return values.stream()
        .map(JsonValue::asJsonArray)
        .reduce(
            pair(MAX_VALUE, 0),
            (p, a) -> pair(min(a.size(), p.first), max(a.size(), p.second)),
            (p1, p2) -> p1);
  }

  private static JsonValue value(
      final List<JsonValue> values,
      final int index,
      final List<JsonValue> defaults,
      final int shortest) {
    return toArray(
        rangeExclusive(0, values.size())
            .map(
                i ->
                    Optional.of(values.get(i).asJsonArray())
                        .filter(array -> index < array.size())
                        .map(array -> array.get(index))
                        .orElseGet(() -> defaults.get(index - shortest))));
  }

  static Implementation zip(final JsonValue value, final Features features) {
    final Implementation defaults = memberFunction(value, DEFAULTS, features);
    final List<Implementation> implementations = memberFunctions(value, INPUTS, features);
    final boolean useLongest = member(value, USE_LONGEST_LENGTH, v -> !isFalse(v)).orElse(false);

    return (json, vars) ->
        applyImplementations(implementations, json, vars)
            .filter(values -> values.stream().allMatch(JsonUtil::isArray))
            .map(values -> pair(values, shortestLongest(values)))
            .map(
                pair ->
                    zip(
                        pair.first,
                        useLongest ? pair.second.second : pair.second.first,
                        pair.second.first,
                        defaults(
                            ofNullable(defaults)
                                .map(d -> d.apply(json, vars))
                                .orElseGet(JsonUtil::emptyArray),
                            pair.second.second)))
            .orElse(NULL);
  }

  private static JsonValue zip(
      final List<JsonValue> values,
      final int end,
      final int shortest,
      final List<JsonValue> defaults) {
    return toArray(rangeExclusive(0, end).map(i -> value(values, i, defaults, shortest)));
  }
}
