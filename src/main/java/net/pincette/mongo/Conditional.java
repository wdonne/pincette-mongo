package net.pincette.mongo;

import static java.util.stream.Collectors.toList;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.mongo.Expression.applyImplementations;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.implementation;
import static net.pincette.mongo.Expression.implementations;
import static net.pincette.mongo.Expression.isFalse;
import static net.pincette.mongo.Expression.member;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Pair.pair;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
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

  private static List<Pair<Implementation, Implementation>> branches(final JsonValue value) {
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
                                implementation(json.getValue("/" + CASE)),
                                implementation(json.getValue("/" + THEN))))
                    .collect(toList()))
        .orElse(null);
  }

  static Implementation cond(final JsonValue value) {
    final List<Implementation> implementations =
        isArray(value)
            ? implementations(value)
            : list(
                memberFunction(value, IF),
                memberFunction(value, THEN),
                memberFunction(value, ELSE));

    return (json, vars) ->
        applyImplementations(
                implementations, json, vars, fncs -> fncs.stream().allMatch(Objects::nonNull))
            .map(values -> values.get(isFalse(values.get(0)) ? 2 : 1))
            .orElse(NULL);
  }

  static Implementation ifNull(final JsonValue value) {
    final List<Implementation> implementations = implementations(value);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .map(values -> values.get(values.get(0).equals(NULL) ? 1 : 0))
            .orElse(NULL);
  }

  static Implementation switchFunction(final JsonValue value) {
    final List<Pair<Implementation, Implementation>> branches =
        member(value, BRANCHES, Conditional::branches).orElse(null);
    final Implementation defaultFunction = memberFunction(value, DEFAULT);

    return (json, vars) ->
        branches != null && defaultFunction != null
            ? branches.stream()
                .map(pair -> pair(pair.first.apply(json, vars), pair.second))
                .filter(pair -> !isFalse(pair.first))
                .map(pair -> pair.second.apply(json, vars))
                .findFirst()
                .orElseGet(() -> defaultFunction.apply(json, vars))
            : NULL;
  }
}
