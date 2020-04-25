package net.pincette.mongo;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.min;
import static java.lang.Math.abs;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isBoolean;
import static net.pincette.json.JsonUtil.isDate;
import static net.pincette.json.JsonUtil.isInstant;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Expression.applyFunctionsNum;
import static net.pincette.mongo.Expression.functions;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.json.JsonObject;
import javax.json.JsonValue;

/**
 * Implements the MongoDB <code>$cmp</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Cmp {
  private Cmp() {}

  static Function<JsonObject, JsonValue> cmp(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctionsNum(functions, json, 2)
            .map(values -> cmp(values.get(0), values.get(1)))
            .orElse(NULL);
  }

  private static JsonValue cmp(final JsonValue v1, final JsonValue v2) {
    return createValue(
        normalize(
            Optional.of(typeValue(v1) - typeValue(v2))
                .filter(result -> result != 0)
                .orElseGet(() -> compare(v1, v2))));
  }

  static boolean comparable(final JsonValue v1, final JsonValue v2) {
    return (isBoolean(v1) && isBoolean(v2))
        || (isNumber(v1) && isNumber(v2))
        || (isString(v1) && isString(v2));
  }

  static int compare(final JsonValue v1, final JsonValue v2) {
    switch (v1.getValueType()) {
      case FALSE:
        return v2.equals(FALSE) ? 0 : -1;
      case NUMBER:
        return compareNumbers(v1, v2);
      case STRING:
        return compareStrings(v1, v2);
      case TRUE:
        return v2.equals(TRUE) ? 0 : 1;
      default:
        return 0;
    }
  }

  static int compareNumbers(final JsonValue v1, final JsonValue v2) {
    return asNumber(v1).bigDecimalValue().compareTo(asNumber(v2).bigDecimalValue());
  }

  static int compareStrings(final JsonValue v1, final JsonValue v2) {
    return asString(v1).getString().compareTo(asString(v2).getString());
  }

  static int normalize(final int result) {
    return min(1, abs(result)) * (result < 0 ? -1 : 1);
  }

  private static int typeValue(final JsonValue value) {
    if (value.equals(NULL)) {
      return 2;
    }

    if (isNumber(value)) {
      return 3;
    }

    if (isString(value) && !isDate(value) && !isInstant(value)) {
      return 4;
    }

    if (isObject(value)) {
      return 5;
    }

    if (isArray(value)) {
      return 6;
    }

    if (value.equals(TRUE) || value.equals(FALSE)) {
      return 9;
    }

    if (isDate(value)) {
      return 10;
    }

    if (isInstant(value)) {
      return 11;
    }

    return MAX_VALUE;
  }
}
