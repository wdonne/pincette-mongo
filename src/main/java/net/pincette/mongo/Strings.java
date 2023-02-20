package net.pincette.mongo;

import static java.lang.Character.isWhitespace;
import static java.lang.Integer.min;
import static java.lang.String.join;
import static java.net.URLDecoder.decode;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.Base64.getDecoder;
import static java.util.Base64.getEncoder;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.regex.Pattern.quote;
import static java.util.stream.Stream.empty;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.from;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Expression.applyImplementations;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.getInteger;
import static net.pincette.mongo.Expression.getString;
import static net.pincette.mongo.Expression.implementation;
import static net.pincette.mongo.Expression.implementations;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.mongo.Expression.stringsOperator;
import static net.pincette.mongo.Match.compileRegex;
import static net.pincette.mongo.Util.normalize;
import static net.pincette.mongo.Util.toArray;
import static net.pincette.util.StreamUtil.rangeInclusive;
import static net.pincette.util.StreamUtil.takeWhile;

import java.util.ArrayList;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Strings {
  private static final String CAPTURES = "captures";
  private static final String CHARS = "chars";
  private static final String FIND = "find";
  private static final String IDX = "idx";
  private static final String INPUT = "input";
  private static final String MATCH = "match";
  private static final String OPTIONS = "options";
  private static final String REGEX = "regex";
  private static final String REPLACEMENT = "replacement";

  private Strings() {}

  static Implementation base64Decode(final JsonValue value, final Features features) {
    final Decoder decoder = getDecoder();

    return string(
        value, s -> createValue(new String(decoder.decode(s.getBytes(UTF_8)), UTF_8)), features);
  }

  static Implementation base64Encode(final JsonValue value, final Features features) {
    final Encoder encoder = getEncoder();

    return string(value, s -> createValue(encoder.encodeToString(s.getBytes(UTF_8))), features);
  }

  private static JsonObject capture(final Matcher matcher) {
    return createObjectBuilder()
        .add(MATCH, matcher.group())
        .add(IDX, matcher.start())
        .add(CAPTURES, toArray(groups(matcher).map(JsonUtil::createValue)))
        .build();
  }

  private static JsonValue captureAll(final Matcher matcher) {
    return toArray(takeWhile(matcher, m -> m, Matcher::find).map(Strings::capture));
  }

  static Implementation concat(final JsonValue value, final Features features) {
    return stringsOperator(value, Strings::concat, features);
  }

  private static JsonValue concat(final List<String> strings) {
    return createValue(join("", strings));
  }

  private static Stream<String> groups(final Matcher matcher) {
    return matcher.groupCount() > 0
        ? rangeInclusive(1, matcher.groupCount()).map(matcher::group)
        : empty();
  }

  static Implementation jsonToString(final JsonValue value, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) -> createValue(JsonUtil.string(implementation.apply(json, vars)));
  }

  static Implementation indexOfCP(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(
                implementations, json, vars, fncs -> fncs.size() >= 2 && fncs.size() <= 4)
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

  static Implementation ltrim(final JsonValue value, final Features features) {
    return trim(value, true, false, features);
  }

  private static Implementation regex(
      final JsonValue value, final Function<Matcher, JsonValue> capture, final Features features) {
    final List<Implementation> implementations = new ArrayList<>();

    implementations.add(memberFunction(value, INPUT, features));
    implementations.add(memberFunction(value, REGEX, features));
    implementations.add(memberFunction(value, OPTIONS, features));

    return (json, vars) ->
        applyImplementations(
                implementations, json, vars, fncs -> fncs.get(0) != null && fncs.get(1) != null)
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

  static Implementation regexFind(final JsonValue value, final Features features) {
    return regex(value, matcher -> matcher.find() ? capture(matcher) : NULL, features);
  }

  static Implementation regexFindAll(final JsonValue value, final Features features) {
    return regex(value, Strings::captureAll, features);
  }

  static Implementation regexMatch(final JsonValue value, final Features features) {
    return regex(value, matcher -> createValue(matcher.find()), features);
  }

  private static Implementation replace(
      final JsonValue value, final boolean all, final Features features) {
    final List<Implementation> implementations = new ArrayList<>();

    implementations.add(memberFunction(value, INPUT, features));
    implementations.add(memberFunction(value, FIND, features));
    implementations.add(memberFunction(value, REPLACEMENT, features));

    return (json, vars) ->
        applyImplementations(
                implementations,
                json,
                vars,
                fncs -> fncs.get(0) != null && fncs.get(1) != null && fncs.get(2) != null)
            .filter(
                values ->
                    isString(values.get(0)) && isString(values.get(1)) && isString(values.get(2)))
            .map(
                values ->
                    createValue(
                        all
                            ? getString(values, 0)
                                .replace(getString(values, 1), getString(values, 2))
                            : getString(values, 0)
                                .replaceFirst(
                                    quote(getString(values, 1)),
                                    quoteReplacement(getString(values, 2)))))
            .orElse(NULL);
  }

  static Implementation replaceAll(final JsonValue value, final Features features) {
    return replace(value, true, features);
  }

  static Implementation replaceOne(final JsonValue value, final Features features) {
    return replace(value, false, features);
  }

  static Implementation rtrim(final JsonValue value, final Features features) {
    return trim(value, false, true, features);
  }

  private static boolean shouldTrim(final char c, final String chars) {
    return (chars != null && chars.indexOf(c) != -1) || (chars == null && isWhitespace(c));
  }

  static Implementation split(final JsonValue value, final Features features) {
    return stringTwo(value, Strings::split, features);
  }

  private static JsonValue split(final String s, final String delimiter) {
    return toArray(stream(s.split(quote(delimiter))).map(JsonUtil::createValue));
  }

  static Implementation strLenCP(final JsonValue value, final Features features) {
    return string(value, s -> createValue(s.length()), features);
  }

  static Implementation strcasecmp(final JsonValue value, final Features features) {
    return stringTwo(
        value, (s1, s2) -> createValue(normalize(s1.compareToIgnoreCase(s2))), features);
  }

  private static Implementation string(
      final JsonValue value, final Function<String, JsonValue> op, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
            .filter(JsonUtil::isString)
            .map(JsonUtil::asString)
            .map(JsonString::getString)
            .map(op)
            .orElse(NULL);
  }

  private static Implementation stringOrNull(
      final JsonValue value, final Function<String, JsonValue> op, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
            .filter(v -> NULL.equals(v) || isString(v))
            .map(v -> NULL.equals(value) ? createValue("") : op.apply(asString(v).getString()))
            .orElse(NULL);
  }

  static Implementation stringToJson(final JsonValue value, final Features features) {
    return string(value, s -> from(s).map(JsonValue.class::cast).orElse(NULL), features);
  }

  private static Implementation stringTwo(
      final JsonValue value,
      final BiFunction<String, String, JsonValue> op,
      final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .filter(values -> isString(values.get(0)) && isString(values.get(1)))
            .map(values -> op.apply(getString(values, 0), getString(values, 1)))
            .orElse(NULL);
  }

  static Implementation substrCP(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 3)
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

  static Implementation toLower(final JsonValue value, final Features features) {
    return stringOrNull(value, s -> createValue(s.toLowerCase()), features);
  }

  static Implementation toUpper(final JsonValue value, final Features features) {
    return stringOrNull(value, s -> createValue(s.toUpperCase()), features);
  }

  static Implementation trim(final JsonValue value, final Features features) {
    return trim(value, true, true, features);
  }

  private static Implementation trim(
      final JsonValue value, final boolean start, final boolean end, final Features features) {
    final List<Implementation> implementations = new ArrayList<>();

    implementations.add(memberFunction(value, INPUT, features));
    implementations.add(memberFunction(value, CHARS, features));

    return (json, vars) ->
        applyImplementations(implementations, json, vars, fncs -> fncs.get(0) != null)
            .filter(
                values ->
                    isString(values.get(0)) && (values.get(1) == null || isString(values.get(1))))
            .map(
                values ->
                    createValue(
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

    for (i = s.length() - 1; i >= 0 && shouldTrim(s.charAt(i), chars); --i)
      ;

    return i + 1;
  }

  private static int trimStart(final String s, final String chars) {
    int i;

    for (i = 0; i < s.length() && shouldTrim(s.charAt(i), chars); ++i)
      ;

    return i;
  }

  static Implementation uriDecode(final JsonValue value, final Features features) {
    return string(value, s -> createValue(decode(s, UTF_8)), features);
  }

  static Implementation uriEncode(final JsonValue value, final Features features) {
    return string(value, s -> createValue(encode(s, UTF_8)), features);
  }
}
