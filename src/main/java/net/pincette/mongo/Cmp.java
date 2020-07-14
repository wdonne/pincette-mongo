package net.pincette.mongo;

import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.isBoolean;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Expression.applyImplementationsNum;
import static net.pincette.mongo.Expression.implementations;

import java.util.List;
import javax.json.JsonValue;

/**
 * Implements the MongoDB <code>$cmp</code> operator.
 *
 * @author Werner Donn\u00e9
 */
class Cmp {
  private Cmp() {}

  static Implementation cmp(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementationsNum(implementations, json, vars, 2)
            .map(values -> cmp(values.get(0), values.get(1)))
            .orElse(NULL);
  }

  private static JsonValue cmp(final JsonValue v1, final JsonValue v2) {
    return createValue(Util.compare(v1, v2));
  }

  static boolean comparable(final JsonValue v1, final JsonValue v2) {
    return (isBoolean(v1) && isBoolean(v2))
        || (isNumber(v1) && isNumber(v2))
        || (isString(v1) && isString(v2));
  }

  static int compare(final JsonValue v1, final JsonValue v2) {
    if (v2.equals(NULL)) {
      return 1;
    }

    switch (v1.getValueType()) {
      case FALSE:
        return v2.equals(FALSE) ? 0 : -1;
      case NULL:
        return -1;
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
}
