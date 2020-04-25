package net.pincette.mongo;

import static java.time.Duration.between;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asInstant;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.isInstant;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.mongo.Expression.applyFunctions;
import static net.pincette.mongo.Expression.applyFunctionsNum;
import static net.pincette.mongo.Expression.bigMath;
import static net.pincette.mongo.Expression.bigMathTwo;
import static net.pincette.mongo.Expression.functions;
import static net.pincette.mongo.Expression.math;
import static net.pincette.mongo.Expression.mathTwo;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Arithmetic {
  private Arithmetic() {}

  static Function<JsonObject, JsonValue> abs(final JsonValue value) {
    return bigMath(value, BigDecimal::abs);
  }

  static Function<JsonObject, JsonValue> add(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json)
            .filter(Arithmetic::isAddArray)
            .map(
                values ->
                    getInstant(values)
                        .map(instant -> addToInstant(instant, values))
                        .orElseGet(() -> sum(values)))
            .orElse(NULL);
  }

  private static JsonValue addToInstant(final Instant instant, final List<JsonValue> values) {
    return createValue(
        values.stream()
            .filter(JsonUtil::isNumber)
            .map(JsonUtil::asNumber)
            .map(JsonNumber::longValue)
            .reduce(instant, Instant::plusMillis, (i1, i2) -> i1)
            .toString());
  }

  static Function<JsonObject, JsonValue> ceil(final JsonValue value) {
    return toLong(math(value, Math::ceil));
  }

  @SuppressWarnings("java:S1874") // Not true.
  static Function<JsonObject, JsonValue> divide(final JsonValue value) {
    return bigMathTwo(value, BigDecimal::divide, false);
  }

  static Function<JsonObject, JsonValue> exp(final JsonValue value) {
    return math(value, Math::exp);
  }

  static Function<JsonObject, JsonValue> floor(final JsonValue value) {
    return toLong(math(value, Math::floor));
  }

  private static Optional<Instant> getInstant(final List<JsonValue> addArray) {
    return addArray.stream().filter(JsonUtil::isInstant).map(JsonUtil::asInstant).findFirst();
  }

  private static boolean isAddArray(final List<JsonValue> array) {
    final List<JsonValue> values = array.stream().filter(v -> !isNumber(v)).collect(toList());

    return values.isEmpty() || (values.size() == 1 && isInstant(values.get(0)));
  }

  static Function<JsonObject, JsonValue> ln(final JsonValue value) {
    return math(value, Math::log);
  }

  static Function<JsonObject, JsonValue> log(final JsonValue value) {
    return mathTwo(value, (v1, v2) -> Math.log(v1) / Math.log(v2), false);
  }

  static Function<JsonObject, JsonValue> log10(final JsonValue value) {
    return math(value, Math::log10);
  }

  static Function<JsonObject, JsonValue> mod(final JsonValue value) {
    return bigMathTwo(value, BigDecimal::remainder, false);
  }

  static Function<JsonObject, JsonValue> multiply(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json -> applyFunctions(functions, json).map(Arithmetic::multiply).orElse(NULL);
  }

  private static JsonValue multiply(final List<JsonValue> values) {
    return setOp(values, BigDecimal::multiply);
  }

  static Function<JsonObject, JsonValue> pow(final JsonValue value) {
    return bigMathTwo(value, (v1, v2) -> pow(v1, v2.intValue()), false);
  }

  private static BigDecimal pow(final BigDecimal value, final int exp) {
    return exp < 0 ? new BigDecimal(1).divide(value.pow(Math.abs(exp))) : value.pow(exp);
  }

  static Function<JsonObject, JsonValue> round(final JsonValue value) {
    return toLong(mathTwo(value, (v1, v2) -> round(v1, v2 != null ? v2.intValue() : 0), true));
  }

  private static double round(final double value, final int place) {
    final double shift = Math.pow(10, Math.abs(place));

    return place > 0 ? (Math.round(value * shift) / shift) : (Math.round(value / shift) * shift);
  }

  private static JsonValue setOp(
      final List<JsonValue> values, final BinaryOperator<BigDecimal> combiner) {
    return values.stream()
        .filter(JsonUtil::isNumber)
        .map(JsonUtil::asNumber)
        .map(JsonNumber::bigDecimalValue)
        .reduce(combiner)
        .map(JsonUtil::createValue)
        .orElse(NULL);
  }

  static Function<JsonObject, JsonValue> sqrt(final JsonValue value) {
    return math(value, Math::sqrt);
  }

  static Function<JsonObject, JsonValue> subtract(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .map(values -> subtract(values.get(0), values.get(1)))
            .orElse(NULL);
  }

  private static JsonValue subtract(final JsonValue v1, final JsonValue v2) {
    final Supplier<JsonValue> tryDates =
        () ->
            isInstant(v2)
                ? createValue(between(asInstant(v2), asInstant(v1)).toMillis())
                : createValue(asInstant(v1).minusMillis(asLong(v2)).toString());

    return isNumber(v1) && isNumber(v2)
        ? createValue(asNumber(v1).bigDecimalValue().subtract(asNumber(v2).bigDecimalValue()))
        : Optional.of(v1)
            .filter(v -> isInstant(v) && (isInstant(v2) || isLong(v2)))
            .map(v -> tryDates.get())
            .orElse(NULL);
  }

  private static JsonValue sum(final List<JsonValue> values) {
    return setOp(values, BigDecimal::add);
  }

  private static Function<JsonObject, JsonValue> toLong(
      final Function<JsonObject, JsonValue> function) {
    return json -> toLong(function.apply(json));
  }

  private static JsonValue toLong(final JsonValue value) {
    return isLong(value) ? createValue(asNumber(value).longValue()) : value;
  }

  static Function<JsonObject, JsonValue> trunc(final JsonValue value) {
    return toLong(mathTwo(value, (v1, v2) -> trunc(v1, v2 != null ? v2.intValue() : 0), true));
  }

  private static double trunc(final double value, final int place) {
    final double shift = Math.pow(10, Math.abs(place));

    return place > 0 ? ((long) (value * shift) / shift) : ((long) (value / shift) * shift);
  }
}
