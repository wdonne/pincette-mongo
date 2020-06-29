package net.pincette.mongo;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static javax.json.JsonValue.NULL;
import static net.pincette.mongo.Expression.math;
import static net.pincette.mongo.Expression.mathTwo;

import java.util.Optional;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Trigonometry {
  private Trigonometry() {}

  static Implementation acos(final JsonValue value) {
    return toDouble(math(value, Math::acos));
  }

  static Implementation acosh(final JsonValue value) {
    return toDouble(math(value, v -> log(v + sqrt(pow(v, 2) - 1))));
  }

  static Implementation asin(final JsonValue value) {
    return toDouble(math(value, Math::asin));
  }

  static Implementation asinh(final JsonValue value) {
    return toDouble(math(value, v -> log(v + sqrt(pow(v, 2) + 1))));
  }

  static Implementation atan(final JsonValue value) {
    return toDouble(math(value, Math::atan));
  }

  static Implementation atanh(final JsonValue value) {
    return toDouble(math(value, v -> 0.5 * log((1 + v) / (1 - v))));
  }

  static Implementation atan2(final JsonValue value) {
    return toDouble(mathTwo(value, Math::atan2, false));
  }

  static Implementation cos(final JsonValue value) {
    return toDouble(math(value, Math::cos));
  }

  static Implementation degreesToRadians(final JsonValue value) {
    return toDouble(math(value, Math::toRadians));
  }

  static Implementation radiansToDegrees(final JsonValue value) {
    return toDouble(math(value, Math::toDegrees));
  }

  static Implementation sin(final JsonValue value) {
    return toDouble(math(value, Math::sin));
  }

  static Implementation tan(final JsonValue value) {
    return toDouble(math(value, Math::tan));
  }

  private static Implementation toDouble(final Implementation implementation) {
    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
            .filter(JsonUtil::isNumber)
            .map(JsonUtil::asNumber)
            .map(JsonNumber::doubleValue)
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }
}
