package net.pincette.mongo;

import static java.math.BigDecimal.ZERO;
import static java.util.Optional.ofNullable;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asInstant;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.isDouble;
import static net.pincette.json.JsonUtil.isInstant;
import static net.pincette.json.JsonUtil.isInt;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.mongo.Expression.applyImplementations;
import static net.pincette.mongo.Expression.implementation;
import static net.pincette.mongo.Expression.isScalar;
import static net.pincette.mongo.Expression.memberFunction;
import static net.pincette.util.Collections.list;
import static net.pincette.util.Util.tryToGetSilent;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Types {
  private static final String ARRAY_TYPE = "array";
  private static final String BOOL = "bool";
  private static final String DATE = "date";
  private static final String DECIMAL = "decimal";
  private static final String DOUBLE = "double";
  private static final String INPUT = "input";
  private static final String INT = "int";
  private static final String LONG = "long";
  private static final String OBJECT = "object";
  private static final String OBJECT_ID = "objectId";
  private static final String ON_ERROR = "onError";
  private static final String ON_NULL = "onNull";
  private static final String STRING_TYPE = "string";
  private static final String TO = "to";

  private Types() {}

  static Implementation convert(final JsonValue value) {
    final List<Implementation> implementations =
        list(
            memberFunction(value, INPUT),
            memberFunction(value, TO),
            ofNullable(memberFunction(value, ON_ERROR)).orElse((json, vars) -> NULL),
            ofNullable(memberFunction(value, ON_NULL)).orElse((json, vars) -> NULL));

    return (json, vars) ->
        applyImplementations(
                implementations, json, vars, fncs -> fncs.get(0) != null && fncs.get(1) != null)
            .filter(
                values ->
                    isScalar(values.get(0))
                        && (isString(values.get(1)) || isNumber(values.get(1)))
                        && isScalar(values.get(2))
                        && isScalar(values.get(3)))
            .map(
                values ->
                    tryToGetSilent(() -> op(typeString(values.get(1))).apply(values.get(0)))
                        .map(result -> result.equals(NULL) ? values.get(3) : result)
                        .orElseGet(() -> values.get(2)))
            .orElse(NULL);
  }

  private static JsonValue convertToBoolean(final JsonValue value) {
    switch (value.getValueType()) {
      case NUMBER:
        return asNumber(value).bigDecimalValue().equals(ZERO) ? FALSE : TRUE;
      case STRING:
        return TRUE;
      default:
        return value;
    }
  }

  private static JsonValue convertToDecimal(final JsonValue value) {
    switch (value.getValueType()) {
      case FALSE:
        return createValue(0);
      case NULL:
      case NUMBER:
        return createValue(asNumber(value).bigDecimalValue());
      case STRING:
        return isInstant(value)
            ? createValue(new BigDecimal(asInstant(value).toEpochMilli()))
            : createValue(new BigDecimal(asString(value).getString()));
      case TRUE:
        return createValue(1);
      default:
        throw new UnsupportedOperationException(value.toString() + " toDecimal");
    }
  }

  private static JsonValue convertToInteger(final JsonValue value) {
    return createValue(asInt(convertToDecimal(value)));
  }

  private static JsonValue convertToLong(final JsonValue value) {
    return createValue(asLong(convertToDecimal(value)));
  }

  private static JsonValue convertToString(final JsonValue value) {
    switch (value.getValueType()) {
      case FALSE:
        return createValue("false");
      case NULL:
      case STRING:
        return value;
      case NUMBER:
        return createValue(
            Optional.of(asNumber(value))
                .filter(JsonNumber::isIntegral)
                .map(JsonNumber::longValue)
                .map(String::valueOf)
                .orElseGet(value::toString));
      case TRUE:
        return createValue("true");
      default:
        throw new UnsupportedOperationException(value.toString() + " toString");
    }
  }

  private static UnaryOperator<JsonValue> op(final String type) {
    switch (type) {
      case BOOL:
        return Types::convertToBoolean;
      case DOUBLE:
      case DECIMAL:
        return Types::convertToDecimal;
      case INT:
        return Types::convertToInteger;
      case LONG:
        return Types::convertToLong;
      case STRING_TYPE:
        return Types::convertToString;
      default:
        throw new UnsupportedOperationException(type);
    }
  }

  static Implementation toBool(final JsonValue value) {
    return toConvert(value, BOOL);
  }

  private static Implementation toConvert(final JsonValue value, final String type) {
    return convert(createObjectBuilder().add(INPUT, value).add(TO, type).build());
  }

  static Implementation toDecimal(final JsonValue value) {
    return toConvert(value, DECIMAL);
  }

  static Implementation toDouble(final JsonValue value) {
    return toConvert(value, DOUBLE);
  }

  static Implementation toInt(final JsonValue value) {
    return toConvert(value, INT);
  }

  private static String toNumberTypeString(final JsonValue value) {
    final Supplier<String> tryDouble = () -> isDouble(value) ? DOUBLE : DECIMAL;
    final Supplier<String> tryLong = () -> isLong(value) ? LONG : tryDouble.get();

    return isInt(value) ? INT : tryLong.get();
  }

  static Implementation toLong(final JsonValue value) {
    return toConvert(value, LONG);
  }

  static Implementation toString(final JsonValue value) {
    return toConvert(value, STRING_TYPE);
  }

  private static String toTypeString(final JsonValue value) {
    switch (value.getValueType()) {
      case ARRAY:
        return ARRAY_TYPE;
      case FALSE:
      case TRUE:
        return BOOL;
      case NUMBER:
        return toNumberTypeString(value);
      case OBJECT:
        return OBJECT;
      case STRING:
        return STRING_TYPE;
      default:
        return null;
    }
  }

  static Implementation type(final JsonValue value) {
    final Implementation implementation = implementation(value);

    return (json, vars) ->
        Optional.of(implementation.apply(json, vars))
            .map(Types::toTypeString)
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  private static String typeString(final JsonValue value) {
    return isString(value) ? asString(value).getString() : typeString(asInt(value));
  }

  private static String typeString(final int code) {
    switch (code) {
      case 1:
        return DOUBLE;
      case 2:
        return STRING_TYPE;
      case 7:
        return OBJECT_ID;
      case 8:
        return BOOL;
      case 9:
        return DATE;
      case 16:
        return INT;
      case 18:
        return LONG;
      case 19:
        return DECIMAL;
      default:
        return null;
    }
  }
}
