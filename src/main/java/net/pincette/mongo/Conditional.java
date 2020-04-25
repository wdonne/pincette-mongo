package net.pincette.mongo;

import static java.util.stream.Collectors.toList;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.mongo.Expression.applyFunctions;
import static net.pincette.mongo.Expression.applyFunctionsNum;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.functions;
import static net.pincette.mongo.Expression.isFalse;
import static net.pincette.mongo.Expression.member;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;

class Conditional {
  private static final String BRANCHES = "branches";
  private static final String CASE = "case";
  private static final String DEFAULT = "default";
  private static final String ELSE = "else";
  private static final String IF = "if";
  private static final String THEN = "then";

  private Conditional() {}

  private static List<Pair<Function<JsonObject, JsonValue>, Function<JsonObject, JsonValue>>>
      branches(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .map(
            array ->
                array.stream()
                    .filter(JsonUtil::isObject)
                    .map(JsonValue::asJsonObject)
                    .filter(json -> json.containsKey(CASE) && json.containsKey(THEN))
                    .map(
                        json ->
                            pair(
                                function(json.getValue("/" + CASE)),
                                function(json.getValue("/" + THEN))))
                    .collect(toList()))
        .orElse(null);
  }

  static Function<JsonObject, JsonValue> cond(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions =
        isArray(value)
            ? functions(value)
            : list(
                memberFunction(value, IF),
                memberFunction(value, THEN),
                memberFunction(value, ELSE));

    return json ->
        applyFunctions(functions, json, fncs -> fncs.stream().allMatch(Objects::nonNull))
            .map(values -> values.get(isFalse(values.get(0)) ? 2 : 1))
            .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> ifNull(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .map(values -> values.get(values.get(0).equals(NULL) ? 1 : 0))
            .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> switchFunction(final JsonValue value) {
    final List<Pair<Function<JsonObject, JsonValue>, Function<JsonObject, JsonValue>>> branches =
        member(value, BRANCHES, Conditional::branches).orElse(null);
    final Function<JsonObject, JsonValue> defaultFunction = memberFunction(value, DEFAULT);

    return json ->
        branches != null && defaultFunction != null
            ? branches.stream()
                .map(pair -> pair(pair.first.apply(json), pair.second))
                .filter(pair -> !isFalse(pair.first))
                .map(pair -> pair.second.apply(json))
                .findFirst()
                .orElseGet(() -> defaultFunction.apply(json))
            : NULL;
  }
}
