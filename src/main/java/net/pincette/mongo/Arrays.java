package net.pincette.mongo;

import static java.lang.Integer.max;
import static javax.json.Json.createArrayBuilder;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asArray;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.emptyArray;
import static net.pincette.json.JsonUtil.isInt;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Expression.applyFunctions;
import static net.pincette.mongo.Expression.applyFunctionsNum;
import static net.pincette.mongo.Expression.arraysOperator;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.functions;
import static net.pincette.mongo.Expression.getString;
import static net.pincette.mongo.Util.toArray;
import static net.pincette.util.Collections.reverse;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.rangeInclusive;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.StreamUtil.zip;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntSupplier;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Arrays {
  private Arrays() {}

  static Function<JsonObject, JsonValue> arrayElemAt(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .filter(
                values ->
                    JsonUtil.isArray(values.get(0))
                        && isInt(values.get(1))
                        && withinRange(asArray(values.get(0)), asInt(values.get(1))))
            .map(values -> arrayElemAt(asArray(values.get(0)), asInt(values.get(1))))
            .orElse(NULL);
  }

  private static JsonValue arrayElemAt(final JsonArray array, final int index) {
    return array.get(index >= 0 ? index : (array.size() + index));
  }

  static Function<JsonObject, JsonValue> arrayToObject(final JsonValue value) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isArray)
            .map(JsonValue::asJsonArray)
            .filter(array -> array.stream().allMatch(v -> isKV(v) || isObjectArray(v)))
            .map(Arrays::arrayToObject)
            .orElse(NULL);
  }

  private static JsonValue arrayToObject(final JsonArray array) {
    return array.stream()
        .reduce(
            createObjectBuilder(),
            (b, v) ->
                Optional.of(v)
                    .filter(Arrays::isKV)
                    .map(JsonValue::asJsonObject)
                    .map(o -> b.add(o.getString("k"), o.getValue("/v")))
                    .orElseGet(
                        () ->
                            Optional.of(v)
                                .map(JsonValue::asJsonArray)
                                .map(a -> b.add(getString(a, 0), a.get(1)))
                                .orElse(b)),
            (b1, b2) -> b1)
        .build();
  }

  static Function<JsonObject, JsonValue> concatArrays(final JsonValue value) {
    return arraysOperator(value, Arrays::concatArrays);
  }

  private static JsonValue concatArrays(final List<JsonArray> array) {
    return toArray(array.stream().flatMap(JsonArray::stream));
  }

  static Function<JsonObject, JsonValue> in(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .filter(values -> JsonUtil.isArray(values.get(1)))
            .map(values -> values.get(1).asJsonArray().contains(values.get(0)))
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> indexOfArray(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json, fncs -> fncs.size() >= 2 && fncs.size() <= 4)
            .filter(
                values ->
                    JsonUtil.isArray(values.get(0))
                        && (values.size() < 3 || isNumber(values.get(2)))
                        && (values.size() < 4 || isNumber(values.get(3))))
            .map(
                values ->
                    indexOfArray(
                        values.get(0).asJsonArray(),
                        values.get(1),
                        values.size() < 3 ? 0 : asInt(values.get(2)),
                        values.size() < 4 ? (values.size() - 1) : asInt(values.get(3))))
            .orElse(NULL);
  }

  private static JsonValue indexOfArray(
      final JsonArray array, final JsonValue value, final int start, final int end) {
    return zip(array.stream(), rangeInclusive(0, end))
        .filter(pair -> pair.second >= start && pair.first.equals(value))
        .findFirst()
        .map(pair -> createValue(pair.second))
        .orElseGet(() -> createValue(-1));
  }

  static Function<JsonObject, JsonValue> isArray(final JsonValue value) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        Optional.of(function.apply(json)).filter(JsonUtil::isArray).map(a -> TRUE).orElse(FALSE);
  }

  private static boolean isKV(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(json -> json.keySet().size() == 2 && json.containsKey("k") && json.containsKey("v"))
        .isPresent();
  }

  private static boolean isObjectArray(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .filter(a -> a.size() == 2 && isString(a.get(0)))
        .isPresent();
  }

  static Function<JsonObject, JsonValue> objectToArray(final JsonValue value) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isObject)
            .map(JsonValue::asJsonObject)
            .map(Arrays::objectToArray)
            .orElse(NULL);
  }

  private static JsonValue objectToArray(final JsonObject json) {
    return json.entrySet().stream()
        .reduce(
            createArrayBuilder(),
            (b, e) -> b.add(createObjectBuilder().add("k", e.getKey()).add("v", e.getValue())),
            (b1, b2) -> b1)
        .build();
  }

  static Function<JsonObject, JsonValue> range(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json, fncs -> fncs.size() == 2 || fncs.size() == 3)
            .filter(
                values ->
                    isInt(values.get(0))
                        && isInt(values.get(1))
                        && (values.size() < 3 || isInt(values.get(2))))
            .map(Arrays::range)
            .orElse(NULL);
  }

  private static JsonValue range(final List<JsonValue> values) {
    final int start = asInt(values.get(0));
    final int end = asInt(values.get(1));
    final int step = values.size() == 3 ? asInt(values.get(2)) : 1;

    return start <= end && step < 0 || start >= end && step > 0
        ? emptyArray()
        : JsonUtil.createValue(rangeExclusive(start, end, Math.abs(step)));
  }

  static Function<JsonObject, JsonValue> reverseArray(final JsonValue value) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isArray)
            .map(JsonValue::asJsonArray)
            .map(array -> (JsonValue) toArray(stream(reverse(array))))
            .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> size(final JsonValue value) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isArray)
            .map(JsonValue::asJsonArray)
            .map(JsonArray::size)
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> slice(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json, fncs -> fncs.size() == 2 || fncs.size() == 3)
            .filter(
                values ->
                    JsonUtil.isArray(values.get(0))
                        && isInt(values.get(1))
                        && (values.size() < 3
                            || (isInt(values.get(2)) && asInt(values.get(2)) >= 0)))
            .map(
                values ->
                    slice(
                        values.get(0).asJsonArray(),
                        slicePosition(values),
                        Math.abs(asInt(values.get(values.size() - 1)))))
            .orElse(NULL);
  }

  private static JsonValue slice(final JsonArray array, final int position, final int n) {
    return toArray(
        zip(array.stream(), rangeExclusive(0, array.size()))
            .filter(pair -> pair.second >= position && pair.second < position + n)
            .map(pair -> pair.first));
  }

  private static int slicePosition(final List<JsonValue> values) {
    final int n = asInt(values.get(values.size() - 1));
    final IntSupplier defaultPosition =
        () -> n >= 0 ? 0 : max(0, asArray(values.get(0)).size() + n);
    final IntSupplier setPosition =
        () ->
            Optional.of(asInt(values.get(1)))
                .filter(p -> p >= 0)
                .orElseGet(() -> max(0, asArray(values.get(0)).size() + asInt(values.get(1))));

    return values.size() == 2 ? defaultPosition.getAsInt() : setPosition.getAsInt();
  }

  private static boolean withinRange(final JsonArray array, final int index) {
    return (index >= 0 && index < array.size()) || (index < 0 && array.size() + index >= 0);
  }
}
