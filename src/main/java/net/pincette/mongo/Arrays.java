package net.pincette.mongo;

import static java.lang.Integer.max;
import static java.util.stream.Collectors.toMap;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asArray;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.emptyArray;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isInt;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.mongo.Expression.applyImplementations;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.arraysOperator;
import static net.pincette.mongo.Expression.getString;
import static net.pincette.mongo.Expression.implementation;
import static net.pincette.mongo.Expression.implementations;
import static net.pincette.mongo.Expression.isFalse;
import static net.pincette.mongo.Expression.member;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.mongo.Expression.replaceVariables;
import static net.pincette.mongo.Util.toArray;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.reverse;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.rangeInclusive;
import static net.pincette.util.StreamUtil.stream;
import static net.pincette.util.StreamUtil.zip;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Arrays {
  private static final String AS = "as";
  private static final String ASC = "asc";
  private static final String COND = "cond";
  private static final String DESC = "desc";
  private static final String DIRECTION = "direction";
  private static final String IN = "in";
  private static final String INITIAL_VALUE = "initialValue";
  private static final String INPUT = "input";
  private static final String PATHS = "paths";
  private static final String THIS = "this";
  private static final String VALUE = "$$value";

  private Arrays() {}

  static Implementation arrayElemAt(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
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

  static Implementation arrayToObject(final JsonValue value, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
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

  private static int compare(final JsonValue v1, final JsonValue v2, final List<String> paths) {
    final BiFunction<JsonValue, String, JsonValue> value =
        (v, p) -> getValue(v.asJsonObject(), toJsonPointer(p)).orElse(NULL);

    return paths.stream()
        .map(path -> Cmp.compare(value.apply(v1, path), value.apply(v2, path)))
        .filter(result -> result != 0)
        .findFirst()
        .orElse(0);
  }

  static Implementation concatArrays(final JsonValue value, final Features features) {
    return arraysOperator(value, Arrays::concatArrays, features);
  }

  private static JsonValue concatArrays(final List<JsonArray> array) {
    return toArray(array.stream().flatMap(JsonArray::stream));
  }

  static Implementation elemMatch(final JsonValue value, final Features features) {
    final Implementation implementation =
        isElemMatch(value) ? implementation(value.asJsonArray().get(0), features) : null;
    final Map<String, Implementation> conditions =
        elemMatchConditions(value.asJsonArray().get(1).asJsonObject(), features);

    return (json, vars) ->
        implementation != null && conditions != null
            ? Optional.of(implementation.apply(json, vars))
                .filter(JsonUtil::isArray)
                .map(JsonValue::asJsonArray)
                .flatMap(
                    array ->
                        Optional.of(elemMatchPredicate(json, vars, conditions, features))
                            .flatMap(predicate -> array.stream().filter(predicate).findFirst()))
                .orElse(NULL)
            : NULL;
  }

  private static Map<String, Implementation> elemMatchConditions(
      final JsonObject expression, final Features features) {
    return expression.entrySet().stream()
        .collect(toMap(Entry::getKey, e -> implementation(e.getValue(), features)));
  }

  private static Predicate<JsonValue> elemMatchPredicate(
      final JsonObject json,
      final Map<String, JsonValue> variables,
      final Map<String, Implementation> conditions,
      final Features features) {
    return Match.elemMatchPredicate(
        conditions.entrySet().stream()
            .reduce(
                createObjectBuilder(),
                (b, e) -> b.add(e.getKey(), e.getValue().apply(json, variables)),
                (b1, b2) -> b1)
            .build(),
        features);
  }

  static Implementation filter(final JsonValue value, final Features features) {
    return mapper(
        value,
        (array, values) ->
            toArray(
                zip(array.stream(), values.stream())
                    .filter(pair -> !isFalse(pair.second))
                    .map(pair -> pair.first)),
        features);
  }

  static Implementation first(final JsonValue value, final Features features) {
    return arrayElemAt(createArrayBuilder().add(value).add(0).build(), features);
  }

  static Implementation in(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .filter(values -> JsonUtil.isArray(values.get(1)))
            .map(values -> values.get(1).asJsonArray().contains(values.get(0)))
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  static Implementation indexOfArray(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(
                implementations, json, vars, fncs -> fncs.size() >= 2 && fncs.size() <= 4)
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

  static Implementation isArray(final JsonValue value, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
            .filter(JsonUtil::isArray)
            .map(a -> TRUE)
            .orElse(FALSE);
  }

  private static boolean isElemMatch(final JsonValue value) {
    return JsonUtil.isArray(value)
        && value.asJsonArray().size() == 2
        && isObject(value.asJsonArray().get(1));
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

  static Implementation last(final JsonValue value, final Features features) {
    return arrayElemAt(createArrayBuilder().add(value).add(-1).build(), features);
  }

  static Implementation mapOp(final JsonValue value, final Features features) {
    return mapper(value, (array, values) -> toArray(values.stream()), features);
  }

  private static Implementation mapper(
      final JsonValue value,
      final BiFunction<JsonArray, List<JsonValue>, JsonValue> combine,
      final Features features) {
    final JsonValue in =
        member(value, IN, v -> v).orElseGet(() -> member(value, COND, v -> v).orElse(null));
    final Implementation input = memberFunction(value, INPUT, features);
    final String variable = member(value, AS, v -> asString(v).getString()).orElse(THIS);

    return (json, vars) ->
        input != null && in != null
            ? Optional.of(input.apply(json, vars))
                .filter(JsonUtil::isArray)
                .map(JsonValue::asJsonArray)
                .map(
                    array ->
                        combine.apply(
                            array,
                            mapper(array, "$$" + variable, in, features).stream()
                                .map(i -> i.apply(json, vars))
                                .toList()))
                .orElse(NULL)
            : NULL;
  }

  private static List<Implementation> mapper(
      final JsonArray values,
      final String variable,
      final JsonValue expression,
      final Features features) {
    return values.stream()
        .map(v -> implementation(replaceVariables(expression, map(pair(variable, v))), features))
        .toList();
  }

  static Implementation objectToArray(final JsonValue value, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
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

  static Implementation range(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(
                implementations, json, vars, fncs -> fncs.size() == 2 || fncs.size() == 3)
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
        : createValue(rangeExclusive(start, end, Math.abs(step)));
  }

  static Implementation reduce(final JsonValue value, final Features features) {
    final JsonValue in = member(value, IN, v -> v).orElse(null);
    final Implementation initial = memberFunction(value, INITIAL_VALUE, features);
    final Implementation input = memberFunction(value, INPUT, features);

    return (json, vars) ->
        input != null && initial != null && in != null
            ? Optional.of(input.apply(json, vars))
                .filter(JsonUtil::isArray)
                .map(JsonValue::asJsonArray)
                .map(
                    array ->
                        array.stream()
                            .reduce(
                                initial.apply(json, vars),
                                (result, v) -> reduce(in, result, v, features).apply(json, vars),
                                (r1, r2) -> r1))
                .orElse(NULL)
            : NULL;
  }

  private static Implementation reduce(
      final JsonValue expression,
      final JsonValue result,
      final JsonValue value,
      final Features features) {
    return implementation(
        replaceVariables(
            replaceVariables(expression, map(pair("$$" + THIS, value))), map(pair(VALUE, result))),
        features);
  }

  static Implementation reverseArray(final JsonValue value, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
            .filter(JsonUtil::isArray)
            .map(JsonValue::asJsonArray)
            .map(array -> toArray(stream(reverse(array))))
            .orElse(NULL);
  }

  static Implementation size(final JsonValue value, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
            .filter(JsonUtil::isArray)
            .map(JsonValue::asJsonArray)
            .map(JsonArray::size)
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  static Implementation slice(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(
                implementations, json, vars, fncs -> fncs.size() == 2 || fncs.size() == 3)
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

  static Implementation sort(final JsonValue value, final Features features) {
    final Optional<JsonObject> object =
        Optional.of(value).filter(JsonUtil::isObject).map(JsonValue::asJsonObject);
    final String direction =
        object
            .map(json -> json.getString(DIRECTION, null))
            .filter(dir -> dir.equals(ASC) || dir.equals(DESC))
            .orElse(ASC);
    final List<String> paths = object.map(json -> getStrings(json, PATHS).toList()).orElse(null);
    final Implementation input = memberFunction(value, INPUT, features);

    return (json, vars) ->
        input != null
            ? Optional.of(input.apply(json, vars))
                .filter(JsonUtil::isArray)
                .map(JsonValue::asJsonArray)
                .map(array -> sort(array, paths, direction))
                .orElse(NULL)
            : NULL;
  }

  private static JsonValue sort(
      final JsonArray array, final List<String> paths, final String direction) {
    return array.stream()
        .filter(v -> paths == null || paths.isEmpty() || isObject(v))
        .sorted(
            (v1, v2) ->
                (paths != null && !paths.isEmpty() ? compare(v1, v2, paths) : Cmp.compare(v1, v2))
                    * (direction.equals(ASC) ? 1 : -1))
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  private static boolean withinRange(final JsonArray array, final int index) {
    return (index >= 0 && index < array.size()) || (index < 0 && array.size() + index >= 0);
  }
}
