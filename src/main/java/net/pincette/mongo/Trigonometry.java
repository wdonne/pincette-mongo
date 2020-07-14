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

  static Implementation acos(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::acos, features));
  }

  static Implementation acosh(final JsonValue value, final Features features) {
    return toDouble(math(value, v -> log(v + sqrt(pow(v, 2) - 1)), features));
  }

  static Implementation asin(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::asin, features));
  }

  static Implementation asinh(final JsonValue value, final Features features) {
    return toDouble(math(value, v -> log(v + sqrt(pow(v, 2) + 1)), features));
  }

  static Implementation atan(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::atan, features));
  }

  static Implementation atanh(final JsonValue value, final Features features) {
    return toDouble(math(value, v -> 0.5 * log((1 + v) / (1 - v)), features));
  }

  static Implementation atan2(final JsonValue value, final Features features) {
    return toDouble(mathTwo(value, Math::atan2, false, features));
  }

  static Implementation cos(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::cos, features));
  }

  static Implementation degreesToRadians(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::toRadians, features));
  }

  static Implementation radiansToDegrees(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::toDegrees, features));
  }

  static Implementation sin(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::sin, features));
  }

  static Implementation tan(final JsonValue value, final Features features) {
    return toDouble(math(value, Math::tan, features));
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
