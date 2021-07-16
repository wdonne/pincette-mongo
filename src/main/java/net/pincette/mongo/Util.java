package net.pincette.mongo;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.min;
import static java.lang.Math.abs;
import static java.util.logging.Logger.getLogger;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isDate;
import static net.pincette.json.JsonUtil.isInstant;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.util.Pair.pair;

import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.util.Pair;

/**
 * Some utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.3.2
 */
public class Util {
  static final Logger logger = getLogger("net.pincette.mongo.expressions");
  private static final String TRACE = "$trace";

  private Util() {}

  /**
   * Compares two values according to the
   *
   * <p><a
   * href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/#bson-types-comparison-order">BSON
   * comparison order</a>
   *
   * @param v1 the first value.
   * @param v2 the second value.
   * @return Returns a negative value if <code>v1</code> is smaller than <code>v2</code>, a positive
   *     value if <code>v1</code> is larger than <code>v2</code> and 0 otherwise.
   * @since 1.3.2
   */
  public static int compare(final JsonValue v1, final JsonValue v2) {
    return normalize(
        Optional.of(typeValue(v1) - typeValue(v2))
            .filter(result -> result != 0)
            .orElseGet(() -> Cmp.compare(v1, v2)));
  }

  static Optional<String> key(final JsonObject expression) {
    return Optional.of(expression.keySet())
        .filter(keys -> keys.size() == 1)
        .map(keys -> keys.iterator().next());
  }

  static int normalize(final int result) {
    return min(1, abs(result)) * (result < 0 ? -1 : 1);
  }

  static JsonValue toArray(final Stream<JsonValue> values) {
    return values.reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1).build();
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

  static Pair<JsonValue, Boolean> unwrapTrace(final JsonValue expression) {
    return isObject(expression)
        ? key(expression.asJsonObject())
            .filter(key -> key.equals(TRACE))
            .map(key -> pair(expression.asJsonObject().getValue("/" + key), true))
            .orElseGet(() -> pair(expression, false))
        : pair(expression, false);
  }

  static Pair<JsonObject, Boolean> unwrapTrace(final JsonObject expression) {
    final Pair<JsonValue, Boolean> unwrapped = unwrapTrace((JsonValue) expression);

    return pair(unwrapped.first.asJsonObject(), unwrapped.second);
  }
}
