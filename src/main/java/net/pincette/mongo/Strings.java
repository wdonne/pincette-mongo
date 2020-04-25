package net.pincette.mongo;

import static java.lang.Character.isWhitespace;
import static java.lang.Integer.min;
import static java.lang.String.join;
import static java.util.Arrays.stream;
import static java.util.regex.Pattern.quote;
import static java.util.stream.Stream.empty;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Cmp.normalize;
import static net.pincette.mongo.Expression.applyFunctions;
import static net.pincette.mongo.Expression.applyFunctionsNum;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.functions;
import static net.pincette.mongo.Expression.getInteger;
import static net.pincette.mongo.Expression.getString;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.mongo.Expression.stringsOperator;
import static net.pincette.mongo.Match.compileRegex;
import static net.pincette.mongo.Util.toArray;
import static net.pincette.util.Collections.list;
import static net.pincette.util.StreamUtil.rangeInclusive;
import static net.pincette.util.StreamUtil.takeWhile;

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Strings {
  private static final String CAPTURES = "captures";
  private static final String CHARS = "chars";
  private static final String IDX = "idx";
  private static final String INPUT = "input";
  private static final String MATCH = "match";
  private static final String OPTIONS = "options";
  private static final String REGEX = "regex";

  private Strings() {}

  private static JsonObject capture(final Matcher matcher) {
    return createObjectBuilder()
        .add(MATCH, matcher.group())
        .add(IDX, matcher.start())
        .add(CAPTURES, toArray(groups(matcher).map(Json::createValue)))
        .build();
  }

  private static JsonArray captureAll(final Matcher matcher) {
    return toArray(takeWhile(matcher, m -> m, Matcher::find).map(Strings::capture));
  }

  static Function<JsonObject, JsonValue> concat(final JsonValue value) {
    return stringsOperator(value, Strings::concat);
  }

  private static JsonValue concat(final List<String> strings) {
    return createValue(join("", strings));
  }

  private static Stream<String> groups(final Matcher matcher) {
    return matcher.groupCount() > 0
        ? rangeInclusive(1, matcher.groupCount()).map(matcher::group)
        : empty();
  }

  static Function<JsonObject, JsonValue> indexOfCP(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json, fncs -> fncs.size() >= 2 && fncs.size() <= 4)
            .filter(
                values ->
                    isString(values.get(0))
                        && isString(values.get(1))
                        && (values.size() < 3 || isPositive(values.get(2)))
                        && (values.size() < 4 || isPositive(values.get(3))))
            .map(
                values ->
                    indexOfCP(
                        getString(values, 0),
                        getString(values, 1),
                        values.size() < 3 ? 0 : asInt(values.get(2)),
                        values.size() < 4
                            ? getString(values, 0).length()
                            : (asInt(values.get(3)) + 1)))
            .orElse(NULL);
  }

  private static JsonValue indexOfCP(
      final String s, final String sub, final int start, final int end) {
    return createValue(
        end < start || end > s.length() || start >= s.length()
            ? -1
            : s.substring(0, end).indexOf(sub, start));
  }

  private static boolean isPositive(final JsonValue value) {
    return isNumber(value) && asInt(value) >= 0;
  }

  static Function<JsonObject, JsonValue> ltrim(final JsonValue value) {
    return trim(value, true, false);
  }

  private static Function<JsonObject, JsonValue> regex(
      final JsonValue value, final Function<Matcher, JsonValue> capture) {
    final List<Function<JsonObject, JsonValue>> functions =
        list(
            memberFunction(value, INPUT),
            memberFunction(value, REGEX),
            memberFunction(value, OPTIONS));

    return json ->
        applyFunctions(functions, json, fncs -> fncs.get(0) != null && fncs.get(1) != null)
            .filter(
                values ->
                    isString(values.get(0))
                        && isString(values.get(1))
                        && (values.get(2) == null || isString(values.get(2))))
            .map(
                values ->
                    capture.apply(
                        compileRegex(
                                getString(values, 1),
                                values.get(2) == null ? null : getString(values, 2))
                            .matcher(getString(values, 0))))
            .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> regexFind(final JsonValue value) {
    return regex(value, matcher -> matcher.find() ? capture(matcher) : NULL);
  }

  static Function<JsonObject, JsonValue> regexFindAll(final JsonValue value) {
    return regex(value, Strings::captureAll);
  }

  static Function<JsonObject, JsonValue> regexMatch(final JsonValue value) {
    return regex(value, matcher -> JsonUtil.createValue(matcher.find()));
  }

  static Function<JsonObject, JsonValue> rtrim(final JsonValue value) {
    return trim(value, false, true);
  }

  private static boolean shouldTrim(final char c, final String chars) {
    return (chars != null && chars.indexOf(c) != -1) || (chars == null && isWhitespace(c));
  }

  static Function<JsonObject, JsonValue> split(final JsonValue value) {
    return stringTwo(value, Strings::split);
  }

  private static JsonValue split(final String s, final String delimiter) {
    return toArray(stream(s.split(quote(delimiter))).map(Json::createValue));
  }

  static Function<JsonObject, JsonValue> strLenCP(final JsonValue value) {
    return string(value, s -> createValue(s.length()));
  }

  static Function<JsonObject, JsonValue> strcasecmp(final JsonValue value) {
    return stringTwo(value, (s1, s2) -> createValue(normalize(s1.compareToIgnoreCase(s2))));
  }

  private static Function<JsonObject, JsonValue> string(
      final JsonValue value, final Function<String, JsonValue> op) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isString)
            .map(JsonUtil::asString)
            .map(JsonString::getString)
            .map(op)
            .orElse(NULL);
  }

  private static Function<JsonObject, JsonValue> stringOrNull(
      final JsonValue value, final Function<String, JsonValue> op) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        Optional.of(function.apply(json))
            .filter(v -> NULL.equals(v) || isString(v))
            .map(v -> NULL.equals(value) ? createValue("") : op.apply(asString(v).getString()))
            .orElse(NULL);
  }

  private static Function<JsonObject, JsonValue> stringTwo(
      final JsonValue value, final BiFunction<String, String, JsonValue> op) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .filter(values -> isString(values.get(0)) && isString(values.get(1)))
            .map(values -> op.apply(getString(values, 0), getString(values, 1)))
            .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> substrCP(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 3)
            .filter(
                values ->
                    (NULL.equals(values.get(0)) || isString(values.get(0)))
                        && isNumber(values.get(1))
                        && isNumber(values.get(2)))
            .map(
                values ->
                    NULL.equals(values.get(0))
                        ? createValue("")
                        : substrCP(
                            getString(values, 0), getInteger(values, 1), getInteger(values, 2)))
            .orElse(NULL);
  }

  private static JsonValue substrCP(final String s, final int index, final int count) {
    return createValue(s.substring(index, min(s.length(), index + count)));
  }

  static Function<JsonObject, JsonValue> toLower(final JsonValue value) {
    return stringOrNull(value, s -> createValue(s.toLowerCase()));
  }

  static Function<JsonObject, JsonValue> toUpper(final JsonValue value) {
    return stringOrNull(value, s -> createValue(s.toUpperCase()));
  }

  static Function<JsonObject, JsonValue> trim(final JsonValue value) {
    return trim(value, true, true);
  }

  private static Function<JsonObject, JsonValue> trim(
      final JsonValue value, final boolean start, final boolean end) {
    final List<Function<JsonObject, JsonValue>> functions =
        list(memberFunction(value, INPUT), memberFunction(value, CHARS));

    return json ->
        applyFunctions(functions, json, fncs -> fncs.get(0) != null)
            .filter(
                values ->
                    isString(values.get(0)) && (values.get(1) == null || isString(values.get(1))))
            .map(
                values ->
                    JsonUtil.createValue(
                        trim(
                            getString(values, 0),
                            values.get(1) != null ? getString(values, 1) : null,
                            start,
                            end)))
            .orElse(NULL);
  }

  private static String trim(
      final String s, final String chars, final boolean start, final boolean end) {
    return s.substring(start ? trimStart(s, chars) : 0, end ? trimEnd(s, chars) : s.length());
  }

  private static int trimEnd(final String s, final String chars) {
    int i;

    for (i = s.length() - 1; i >= 0 && shouldTrim(s.charAt(i), chars); --i) ;

    return i + 1;
  }

  private static int trimStart(final String s, final String chars) {
    int i;

    for (i = 0; i < s.length() && shouldTrim(s.charAt(i), chars); ++i) ;

    return i;
  }
}
