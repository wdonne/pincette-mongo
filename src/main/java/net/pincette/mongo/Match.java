package net.pincette.mongo;

import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.COMMENTS;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.MULTILINE;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Stream.of;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asArray;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.asLong;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isDate;
import static net.pincette.json.JsonUtil.isDouble;
import static net.pincette.json.JsonUtil.isInstant;
import static net.pincette.json.JsonUtil.isInt;
import static net.pincette.json.JsonUtil.isLong;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.isString;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Cmp.compareNumbers;
import static net.pincette.mongo.Cmp.compareStrings;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Expression.isFalse;
import static net.pincette.mongo.Util.LOGGER;
import static net.pincette.mongo.Util.key;
import static net.pincette.mongo.Util.unwrapTrace;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.mongo.Relational.RelOp;
import net.pincette.util.Pair;
import org.bson.conversions.Bson;

/**
 * You can match JSON objects with <a
 * href="https://docs.mongodb.com/manual/reference/operator/query/">MongoDB queries</a> using this
 * class. The following operators are not supported: <code>$jsonSchema</code>, <code>$text</code>,
 * <code>$where</code> and the geospatial operators. The operator <code>$expr</code> should contain
 * only expressions which yield a boolean or aggregation expressions. The operator <code>$all</code>
 * doesn't work with the <code>$elemMatch</code> operator.
 *
 * <p>If you wrap an expression in the <code>$trace</code> operator then tracing will be done for it
 * in the logger "net.pincette.mongo.expressions" at level <code>INFO</code>.
 *
 * @see Expression
 * @author Werner Donné
 * @since 1.2
 */
public class Match {
  private static final String ALL = "$all";
  private static final String AND = "$and";
  private static final String ARRAY = "array";
  private static final String BITS_ALL_CLEAR = "$bitsAllClear";
  private static final String BITS_ALL_SET = "$bitsAllSet";
  private static final String BITS_ANY_CLEAR = "$bitsAnyClear";
  private static final String BITS_ANY_SET = "$bitsAnySet";
  private static final String BOOL = "bool";
  private static final String DATE = "date";
  private static final String DECIMAL = "decimal";
  private static final String DOUBLE = "double";
  private static final String ELEM_MATCH = "$elemMatch";
  private static final String EQ = "$eq";
  private static final String EXISTS = "$exists";
  private static final String EXPR = "$expr";
  private static final String GT = "$gt";
  private static final String GTE = "$gte";
  private static final String IN = "$in";
  private static final String INT = "int";
  private static final String LONG = "long";
  private static final String LT = "$lt";
  private static final String LTE = "$lte";
  private static final String MOD = "$mod";
  private static final String NE = "$ne";
  private static final String NIN = "$nin";
  private static final String NOR = "$nor";
  private static final String NOT = "$not";
  private static final String NULL_TYPE = "null";
  private static final String OBJECT = "object";
  private static final String OPTIONS = "$options";
  private static final String OR = "$or";
  private static final String REGEX = "$regex";
  private static final String SIZE = "$size";
  private static final String STRING = "string";
  private static final String TIMESTAMP = "timestamp";
  private static final String TYPE = "$type";
  private static final Set<String> COMBINERS = set(AND, EXPR, NOR, OR);
  private static final Set<String> SUPPORTED_TYPES =
      set(ARRAY, BOOL, DATE, DECIMAL, DOUBLE, INT, LONG, NULL_TYPE, OBJECT, STRING, TIMESTAMP);
  private static final Map<String, QueryOperator> QUERY_OPERATORS =
      map(
          pair(ALL, (e, f) -> all(e)),
          pair(BITS_ALL_CLEAR, (e, f) -> bitsAllClear(e)),
          pair(BITS_ALL_SET, (e, f) -> bitsAllSet(e)),
          pair(BITS_ANY_CLEAR, (e, f) -> bitsAnyClear(e)),
          pair(BITS_ANY_SET, (e, f) -> bitsAnySet(e)),
          pair(ELEM_MATCH, Match::elemMatch),
          pair(EQ, (e, f) -> eq(e)),
          pair(EXISTS, (e, f) -> exists(e)),
          pair(GT, (e, f) -> gt(e)),
          pair(GTE, (e, f) -> gte(e)),
          pair(IN, (e, f) -> in(e)),
          pair(LT, (e, f) -> lt(e)),
          pair(LTE, (e, f) -> lte(e)),
          pair(MOD, (e, f) -> mod(e)),
          pair(NE, (e, f) -> ne(e)),
          pair(NIN, (e, f) -> nin(e)),
          pair(NOT, Match::not),
          pair(SIZE, (e, f) -> size(e)),
          pair(TYPE, (e, f) -> type(e)));

  private Match() {}

  private static Predicate<JsonObject> aggregationExpression(
      final JsonValue value, final Features features) {
    final Function<JsonObject, JsonValue> function = function(value, features);

    return json -> !isFalse(function.apply(json));
  }

  private static Predicate<JsonObject> and(final JsonValue value, final Features features) {
    return combine(value, (p1, p2) -> (json -> p1.test(json) && p2.test(json)), features);
  }

  private static Predicate<JsonValue> all(final JsonValue value) {
    final JsonArray array = isArray(value) ? asArray(value) : null;

    return v -> v != null && array != null && isArray(v) && hasAllValues(asArray(v), array);
  }

  private static Predicate<JsonValue> bits(
      final JsonValue value, final BiPredicate<Long, Long> op) {
    final Optional<Long> mask = mask(value);

    return v ->
        v != null
            && mask.isPresent()
            && isNumber(v)
            && op.test(asNumber(v).longValue(), mask.get());
  }

  private static Predicate<JsonValue> bitsAllClear(final JsonValue value) {
    return bits(value, (v, mask) -> (v & mask) == 0);
  }

  private static Predicate<JsonValue> bitsAllSet(final JsonValue value) {
    return bits(value, (v, mask) -> (v & mask) == mask);
  }

  private static Predicate<JsonValue> bitsAnyClear(final JsonValue value) {
    return bits(value, (v, mask) -> (v & mask) != mask);
  }

  private static Predicate<JsonValue> bitsAnySet(final JsonValue value) {
    return bits(value, (v, mask) -> (v & mask) != 0);
  }

  private static Predicate<JsonObject> booleanExpression(
      final JsonObject expression, final Features features) {
    return key(expression)
        .map(key -> booleanExpression(key, expression.getValue("/" + key), features))
        .orElseGet(() -> aggregationExpression(expression, features));
  }

  private static Predicate<JsonObject> booleanExpression(
      final String key, final JsonValue value, final Features features) {
    return switch (key) {
      case AND -> and(value, features);
      case EQ -> booleanExpression(Relational::eq, value, features);
      case GT -> booleanExpression(Relational::gt, value, features);
      case GTE -> booleanExpression(Relational::gte, value, features);
      case LT -> booleanExpression(Relational::lt, value, features);
      case LTE -> booleanExpression(Relational::lte, value, features);
      case NE -> booleanExpression(Relational::ne, value, features);
      case NOR -> nor(value, features);
      case OR -> or(value, features);
      default -> null;
    };
  }

  private static Predicate<JsonObject> booleanExpression(
      final BiFunction<JsonValue, Features, RelOp> op,
      final JsonValue value,
      final Features features) {
    final RelOp relOp = op.apply(value, features);
    final Map<String, JsonValue> variables = emptyMap();

    return json -> relOp.test(json, variables);
  }

  private static Predicate<JsonObject> combine(
      final JsonValue value,
      BinaryOperator<Predicate<JsonObject>> combiner,
      final Features features) {
    return isArray(value)
        ? value.asJsonArray().stream()
            .filter(JsonUtil::isObject)
            .map(JsonValue::asJsonObject)
            .map(json -> predicate(json, features))
            .reduce(combiner)
            .orElse(json -> false)
        : json -> false;
  }

  static Pattern compileRegex(final String regex, final String options) {
    return compile(pattern(regex), flags(flagsFromRegex(regex).orElse(options)));
  }

  private static Predicate<JsonValue> elemMatch(final JsonValue value, final Features features) {
    final Predicate<JsonValue> predicate = elemMatchPredicate(value, features);

    return v -> v != null && isArray(v) && asArray(v).stream().anyMatch(predicate);
  }

  static Predicate<JsonValue> elemMatchPredicate(final JsonValue value, final Features features) {
    return Optional.of(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(JsonObject::entrySet)
        .flatMap(
            entries ->
                entries.stream()
                    .map(entry -> elemMatchPredicate(entry.getKey(), entry.getValue(), features))
                    .reduce((p1, p2) -> (v -> p1.test(v) && p2.test(v))))
        .orElseGet(Match::falsePredicate);
  }

  private static Predicate<JsonValue> elemMatchPredicate(
      final String key, final JsonValue value, final Features features) {
    return ofNullable(predicate(key, value, features))
        .orElseGet(
            () -> {
              final Predicate<JsonObject> predicate = predicateField(key, value, features);

              return json -> isObject(json) && predicate.test(json.asJsonObject());
            });
  }

  private static Predicate<JsonValue> eq(final JsonValue value) {
    return value::equals;
  }

  private static Predicate<JsonValue> exists(final JsonValue value) {
    final Supplier<Predicate<JsonValue>> tryTrue =
        () -> TRUE.equals(value) ? Objects::nonNull : v -> false;

    return FALSE.equals(value) ? Objects::isNull : tryTrue.get();
  }

  private static Predicate<JsonObject> expr(final JsonValue value, final Features features) {
    return isObject(value) ? booleanExpression(value.asJsonObject(), features) : json -> false;
  }

  private static Predicate<JsonValue> falsePredicate() {
    return v -> false;
  }

  static int flags(final String options) {
    final IntSupplier i = () -> options.contains("i") ? CASE_INSENSITIVE : 0;
    final IntSupplier m = () -> options.contains("m") ? MULTILINE : 0;
    final IntSupplier s = () -> options.contains("s") ? DOTALL : 0;
    final IntSupplier x = () -> options.contains("x") ? COMMENTS : 0;

    return options == null ? 0 : (i.getAsInt() | m.getAsInt() | s.getAsInt() | x.getAsInt());
  }

  private static Optional<String> flagsFromRegex(final String regex) {
    return Optional.of(regex.lastIndexOf('/'))
        .filter(index -> index != -1 && index < regex.length() - 1)
        .map(index -> regex.substring(index + 1));
  }

  private static Optional<JsonArray> getNestedArray(final JsonArray array) {
    return Optional.of(array)
        .filter(a -> a.size() == 1)
        .map(a -> a.get(0))
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray);
  }

  private static Pattern getRegex(final JsonObject json) {
    return ofNullable(json.getString(REGEX, null))
        .map(regex -> compileRegex(regex, json.getString(OPTIONS, null)))
        .orElse(null);
  }

  private static Optional<Predicate<JsonValue>> getRegex(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isString)
        .map(JsonUtil::asString)
        .map(JsonString::getString)
        .filter(Match::isRegexp)
        .map(v -> compileRegex(v, null))
        .map(pattern -> (v -> matches(pattern, v)));
  }

  private static Predicate<JsonValue> gt(final JsonValue value) {
    return v ->
        switch (value.getValueType()) {
          case FALSE -> v != null && v.equals(TRUE);
          case NUMBER -> v != null && isNumber(v) && compareNumbers(v, value) > 0;
          case STRING -> v != null && isString(v) && compareStrings(v, value) > 0;
          default -> false;
        };
  }

  private static Predicate<JsonValue> gte(final JsonValue value) {
    return v ->
        switch (value.getValueType()) {
          case FALSE -> v != null && (v.equals(FALSE) || v.equals(TRUE));
          case NUMBER -> v != null && isNumber(v) && compareNumbers(v, value) >= 0;
          case STRING -> v != null && isString(v) && compareStrings(v, value) >= 0;
          case TRUE -> TRUE.equals(v);
          default -> false;
        };
  }

  private static boolean hasAllValues(final JsonArray v, final JsonArray array) {
    return hasAllValuesDirect(v, array)
        || getNestedArray(array)
            .filter(a -> hasAllValuesDirect(v, a) || hasAllValuesNested(v, a))
            .isPresent();
  }

  private static boolean hasAllValuesDirect(final JsonArray v, final JsonArray array) {
    return new HashSet<>(v).containsAll(array);
  }

  private static boolean hasAllValuesNested(final JsonArray v, final JsonArray nestedArray) {
    return v.stream()
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .anyMatch(a -> hasAllValuesDirect(a, nestedArray));
  }

  private static Predicate<JsonValue> in(final JsonValue value) {
    final List<Predicate<JsonValue>> values =
        (isArray(value) ? value.asJsonArray() : Collections.<JsonValue>emptyList())
            .stream().map(v -> getRegex(v).orElse(v::equals)).toList();

    return v -> v != null && values.stream().anyMatch(predicate -> predicate.test(v));
  }

  private static boolean isExpression(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(JsonObject::keySet)
        .filter(
            keys ->
                (keys.size() == 1 && keys.iterator().next().startsWith("$"))
                    || isRegexp(keys, value.asJsonObject()))
        .isPresent();
  }

  private static boolean isRegexp(final Set<String> keys, final JsonObject expression) {
    return keys.size() == 2
        && keys.contains(REGEX)
        && keys.contains(OPTIONS)
        && Optional.of(expression.getString(OPTIONS))
            .filter(options -> options.isEmpty() || options.matches("[imsx]+"))
            .isPresent();
  }

  private static boolean isRegexp(final String s) {
    return s.startsWith("/") && s.lastIndexOf('/') > 0;
  }

  private static boolean isType(final JsonValue value, final String type) {
    return switch (type) {
      case ARRAY -> isArray(value);
      case BOOL -> value.equals(FALSE) || value.equals(TRUE);
      case DATE -> isDate(value);
      case INT -> isInt(value);
      case LONG -> isLong(value);
      case DOUBLE -> isDouble(value);
      case DECIMAL -> isNumber(value);
      case NULL_TYPE -> value.equals(NULL);
      case OBJECT -> isObject(value);
      case TIMESTAMP -> isInstant(value);
      case STRING -> isString(value);
      default -> false;
    };
  }

  private static boolean log(
      final JsonObject expression, final JsonObject json, final boolean result, final Level level) {
    LOGGER.log(
        level,
        () ->
            "Expression:\n"
                + string(expression)
                + "\n\nWith:\n"
                + string(json)
                + "\n\nYields: "
                + result
                + "\n\n");

    return result;
  }

  /**
   * If you set the log level to FINEST you will get a trace.
   *
   * @return The logger with the name "net.pincette.mongo.expression".
   * @since 1.3
   */
  public static Logger logger() {
    return LOGGER;
  }

  private static Predicate<JsonValue> lt(final JsonValue value) {
    return v ->
        switch (value.getValueType()) {
          case NUMBER -> v != null && isNumber(v) && compareNumbers(v, value) < 0;
          case STRING -> v != null && isString(v) && compareStrings(v, value) < 0;
          case TRUE -> v != null && v.equals(FALSE);
          default -> false;
        };
  }

  private static Predicate<JsonValue> lte(final JsonValue value) {
    return v ->
        switch (value.getValueType()) {
          case FALSE -> FALSE.equals(v);
          case NUMBER -> v != null && isNumber(v) && compareNumbers(v, value) <= 0;
          case STRING -> v != null && isString(v) && compareStrings(v, value) <= 0;
          case TRUE -> v != null && (v.equals(FALSE) || v.equals(TRUE));
          default -> false;
        };
  }

  private static Optional<Long> mask(final JsonValue value) {
    return tryWith(
            () ->
                Optional.of(value)
                    .filter(JsonUtil::isNumber)
                    .map(JsonUtil::asNumber)
                    .map(JsonNumber::longValue)
                    .orElse(null))
        .or(
            () ->
                Optional.of(value)
                    .filter(JsonUtil::isArray)
                    .map(JsonValue::asJsonArray)
                    .map(Match::mask)
                    .orElse(null))
        .get();
  }

  private static long mask(final JsonArray positions) {
    return positions.stream()
        .filter(JsonUtil::isNumber)
        .map(JsonUtil::asNumber)
        .map(JsonNumber::longValue)
        .filter(p -> p < 64)
        .reduce(0L, (r, p) -> r | (1L << p), (r1, r2) -> r1);
  }

  private static boolean matches(final Pattern pattern, final JsonValue value) {
    return isString(value) && pattern.matcher(asString(value).getString()).find();
  }

  private static Predicate<JsonValue> mod(final JsonValue value) {
    final Function<Pair<Long, Long>, Predicate<JsonValue>> mod =
        pair ->
            (v ->
                v != null && isNumber(v) && (asNumber(v).longValue() % pair.first == pair.second));

    return Optional.of(value)
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .filter(array -> array.size() == 2)
        .filter(array -> isLong(array.get(0)) && isLong(array.get(1)))
        .map(array -> pair(asLong(array.get(0)), asLong(array.get(1))))
        .map(mod)
        .orElseGet(Match::falsePredicate);
  }

  private static Predicate<JsonValue> ne(final JsonValue value) {
    return v -> !value.equals(v);
  }

  private static Predicate<JsonValue> nin(final JsonValue value) {
    final Predicate<JsonValue> in = in(value);

    return v -> !in.test(v);
  }

  private static Predicate<JsonObject> nor(final JsonValue value, final Features features) {
    return combine(value, (p1, p2) -> (json -> !p1.test(json) && !p2.test(json)), features);
  }

  private static Predicate<JsonValue> not(final JsonValue value, final Features features) {
    final Predicate<JsonValue> predicate =
        isObject(value)
            ? predicateValue(value.asJsonObject(), features)
            : getRegex(value).orElse(v -> true);

    return v -> !predicate.test(v);
  }

  private static Predicate<JsonObject> or(final JsonValue value, final Features features) {
    return combine(value, (p1, p2) -> (json -> p1.test(json) || p2.test(json)), features);
  }

  private static String pattern(final String regex) {
    return regex.substring(
        regex.startsWith("/") ? 1 : 0,
        Optional.of(regex.lastIndexOf('/'))
            .filter(index -> index != -1 && regex.startsWith("/"))
            .orElseGet(regex::length));
  }

  /**
   * Constructs a predicate with <code>expression</code>.
   *
   * @param expression the MongoDB query.
   * @return The predicate, which is stateless.
   * @since 1.2
   */
  public static Predicate<JsonObject> predicate(final JsonObject expression) {
    return predicate(expression, null);
  }

  /**
   * Constructs a predicate with <code>expression</code>.
   *
   * @param expression the MongoDB query.
   * @param features extra features. It may be <code>null</code>.
   * @return The predicate, which is stateless.
   * @since 2.0
   */
  public static Predicate<JsonObject> predicate(
      final JsonObject expression, final Features features) {
    final Pair<JsonObject, Boolean> unwrapped = unwrapTrace(expression);
    final Function<String, JsonValue> value = key -> unwrapped.first.getValue("/" + key);

    return wrapLogging(
        key(unwrapped.first)
            .map(
                key ->
                    COMBINERS.contains(key)
                        ? predicateCombiner(key, value.apply(key), features)
                        : predicateField(key, value.apply(key), features))
            .orElseGet(() -> predicateFields(unwrapped.first, features)),
        unwrapped.first,
        Boolean.TRUE.equals(unwrapped.second) ? INFO : FINEST);
  }

  /**
   * Constructs a predicate with <code>expression</code>.
   *
   * @param expression the MongoDB query.
   * @return The predicate, which is stateless.
   * @since 1.2
   */
  public static Predicate<JsonObject> predicate(final Bson expression) {
    return predicate(expression, null);
  }

  /**
   * Constructs a predicate with <code>expression</code>.
   *
   * @param expression the MongoDB query.
   * @param features extra features. It may be <code>null</code>.
   * @return The predicate, which is stateless.
   * @since 2.0
   */
  public static Predicate<JsonObject> predicate(final Bson expression, final Features features) {
    return predicate(fromBson(toBsonDocument(expression)), features);
  }

  private static Predicate<JsonValue> predicate(
      final String key, final JsonValue value, final Features features) {
    return tryWith(() -> QUERY_OPERATORS.get(key))
        .or(
            () ->
                ofNullable(features).map(f -> f.matchExtensions).map(e -> e.get(key)).orElse(null))
        .get()
        .map(fn -> fn.apply(value, features))
        .orElse(null);
  }

  private static Predicate<JsonObject> predicateCombiner(
      final String key, final JsonValue value, final Features features) {
    return switch (key) {
      case AND -> and(value, features);
      case EXPR -> expr(value, features);
      case NOR -> nor(value, features);
      case OR -> or(value, features);
      default -> json -> false;
    };
  }

  private static Predicate<JsonObject> predicateField(
      final String field, final JsonValue value, final Features features) {
    final Predicate<JsonValue> predicate =
        isExpression(value) ? predicateValue(value.asJsonObject(), features) : eq(value);

    return json -> predicate.test(getValue(json, toJsonPointer(field)).orElse(null));
  }

  private static Predicate<JsonObject> predicateFields(
      final JsonObject expression, final Features features) {
    return expression.keySet().stream()
        .filter(key -> !key.startsWith("$"))
        .map(key -> predicateField(key, expression.getValue("/" + key), features))
        .reduce((p1, p2) -> (json -> p1.test(json) && p2.test(json)))
        .orElseGet(() -> (json -> false));
  }

  /**
   * Constructs a predicate to test a JSON value.
   *
   * @param expression the MongoDB query.
   * @param features extra features. It may be <code>null</code>.
   * @return The predicate, which is stateless.
   * @since 2.0
   */
  public static Predicate<JsonValue> predicateValue(
      final JsonObject expression, final Features features) {
    return Optional.of(expression.keySet())
        .filter(keys -> keys.size() == 1)
        .map(keys -> keys.iterator().next())
        .map(key -> predicate(key, expression.getValue("/" + key), features))
        .orElseGet(() -> regex(expression));
  }

  private static Predicate<JsonValue> regex(final JsonObject json) {
    final Pattern pattern = getRegex(json);

    return v -> v != null && pattern != null && matches(pattern, v);
  }

  private static Predicate<JsonValue> size(final JsonValue value) {
    final int size = isInt(value) ? asInt(value) : -1;

    return v -> v != null && size != -1 && isArray(v) && v.asJsonArray().size() == size;
  }

  private static List<String> supportedTypes(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isArray)
        .map(JsonValue::asJsonArray)
        .map(JsonArray::stream)
        .orElseGet(() -> of(value))
        .filter(JsonUtil::isString)
        .map(JsonUtil::asString)
        .map(JsonString::getString)
        .filter(SUPPORTED_TYPES::contains)
        .toList();
  }

  private static Predicate<JsonValue> type(final JsonValue value) {
    final List<String> types = supportedTypes(value);

    return v -> v != null && types.stream().anyMatch(t -> isType(v, t));
  }

  private static Predicate<JsonObject> wrapLogging(
      final Predicate<JsonObject> predicate, final JsonObject expression, final Level level) {
    return json ->
        Optional.of(predicate.test(json))
            .map(result -> log(expression, json, result, level))
            .orElse(false);
  }
}
