package net.pincette.mongo;

import static java.lang.Math.log;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.NULL;
import static net.pincette.mongo.Expression.math;
import static net.pincette.mongo.Expression.mathTwo;

import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Trigonometry {
  private Trigonometry() {}

  static Function<JsonObject, JsonValue> acos(final JsonValue value) {
    return toDouble(math(value, Math::acos));
  }

  static Function<JsonObject, JsonValue> acosh(final JsonValue value) {
    return toDouble(math(value, v -> log(v + sqrt(pow(v, 2) - 1))));
  }

  static Function<JsonObject, JsonValue> asin(final JsonValue value) {
    return toDouble(math(value, Math::asin));
  }

  static Function<JsonObject, JsonValue> asinh(final JsonValue value) {
    return toDouble(math(value, v -> log(v + sqrt(pow(v, 2) + 1))));
  }

  static Function<JsonObject, JsonValue> atan(final JsonValue value) {
    return toDouble(math(value, Math::atan));
  }

  static Function<JsonObject, JsonValue> atanh(final JsonValue value) {
    return toDouble(math(value, v -> 0.5 * log((1 + v) / (1 - v))));
  }

  static Function<JsonObject, JsonValue> atan2(final JsonValue value) {
    return toDouble(mathTwo(value, Math::atan2, false));
  }

  static Function<JsonObject, JsonValue> cos(final JsonValue value) {
    return toDouble(math(value, Math::cos));
  }

  static Function<JsonObject, JsonValue> degreesToRadians(final JsonValue value) {
    return toDouble(math(value, Math::toRadians));
  }

  static Function<JsonObject, JsonValue> radiansToDegrees(final JsonValue value) {
    return toDouble(math(value, Math::toDegrees));
  }

  static Function<JsonObject, JsonValue> sin(final JsonValue value) {
    return toDouble(math(value, Math::sin));
  }

  static Function<JsonObject, JsonValue> tan(final JsonValue value) {
    return toDouble(math(value, Math::tan));
  }

  private static Function<JsonObject, JsonValue> toDouble(
      final Function<JsonObject, JsonValue> function) {
    return json ->
        Optional.of(function.apply(json))
            .filter(JsonUtil::isNumber)
            .map(JsonUtil::asNumber)
            .map(JsonNumber::doubleValue)
            .map(value -> (JsonValue) createValue(value))
            .orElse(NULL);
  }
}
