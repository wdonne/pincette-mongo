package net.pincette.mongo;

import static java.time.Duration.between;
import static java.util.stream.Collectors.toList;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asInstant;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.isInstant;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.mongo.Expression.applyImplementations;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.bigMath;
import static net.pincette.mongo.Expression.bigMathTwo;
import static net.pincette.mongo.Expression.implementations;
import static net.pincette.mongo.Expression.math;
import static net.pincette.mongo.Expression.mathTwo;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Arithmetic {
  private Arithmetic() {}

  static Implementation abs(final JsonValue value, final Features features) {
    return bigMath(value, BigDecimal::abs, features);
  }

  static Implementation add(final JsonValue value, final Features features) {
    final List<Implementation> functions = implementations(value, features);

    return (json, vars) ->
        applyImplementations(functions, json, vars)
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

  static Implementation ceil(final JsonValue value, final Features features) {
    return toLong(math(value, Math::ceil, features));
  }

  static Implementation divide(final JsonValue value, final Features features) {
    return bigMathTwo(value, BigDecimal::divide, false, features);
  }

  static Implementation exp(final JsonValue value, final Features features) {
    return math(value, Math::exp, features);
  }

  static Implementation floor(final JsonValue value, final Features features) {
    return toLong(math(value, Math::floor, features));
  }

  private static Optional<Instant> getInstant(final List<JsonValue> addArray) {
    return addArray.stream().filter(JsonUtil::isInstant).map(JsonUtil::asInstant).findFirst();
  }

  private static boolean isAddArray(final List<JsonValue> array) {
    final List<JsonValue> values = array.stream().filter(v -> !isNumber(v)).collect(toList());

    return values.isEmpty() || (values.size() == 1 && isInstant(values.get(0)));
  }

  static Implementation ln(final JsonValue value, final Features features) {
    return math(value, Math::log, features);
  }

  static Implementation log(final JsonValue value, final Features features) {
    return mathTwo(value, (v1, v2) -> Math.log(v1) / Math.log(v2), false, features);
  }

  static Implementation log10(final JsonValue value, final Features features) {
    return math(value, Math::log10, features);
  }

  static Implementation mod(final JsonValue value, final Features features) {
    return bigMathTwo(value, BigDecimal::remainder, false, features);
  }

  static Implementation multiply(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(implementations, json, vars).map(Arithmetic::multiply).orElse(NULL);
  }

  private static JsonValue multiply(final List<JsonValue> values) {
    return setOp(values, BigDecimal::multiply);
  }

  static Implementation pow(final JsonValue value, final Features features) {
    return bigMathTwo(value, (v1, v2) -> pow(v1, v2.intValue()), false, features);
  }

  private static BigDecimal pow(final BigDecimal value, final int exp) {
    return exp < 0 ? new BigDecimal(1).divide(value.pow(Math.abs(exp))) : value.pow(exp);
  }

  static Implementation round(final JsonValue value, final Features features) {
    return toLong(
        mathTwo(value, (v1, v2) -> round(v1, v2 != null ? v2.intValue() : 0), true, features));
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

  static Implementation sqrt(final JsonValue value, final Features features) {
    return math(value, Math::sqrt, features);
  }

  static Implementation subtract(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
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

  private static Implementation toLong(final Implementation implementation) {
    return (json, vars) -> toLong(implementation.apply(json, vars));
  }

  private static JsonValue toLong(final JsonValue value) {
    return isLong(value) ? createValue(asNumber(value).longValue()) : value;
  }

  static Implementation trunc(final JsonValue value, final Features features) {
    return toLong(
        mathTwo(value, (v1, v2) -> trunc(v1, v2 != null ? v2.intValue() : 0), true, features));
  }

  private static double trunc(final double value, final int place) {
    final double shift = Math.pow(10, Math.abs(place));

    return place > 0 ? ((long) (value * shift) / shift) : ((long) (value / shift) * shift);
  }
}
