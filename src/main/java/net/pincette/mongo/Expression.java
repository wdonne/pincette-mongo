package net.pincette.mongo;

import static java.time.Instant.now;
import static java.util.stream.Collectors.toList;
import static javax.json.Json.createObjectBuilder;
import static javax.json.Json.createValue;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Relational.asFunction;
import static net.pincette.mongo.Util.key;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Pair.pair;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.util.Or;
import org.bson.conversions.Bson;

/**
 * This class lets you apply a <a
 * href="https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#expressions">MongoDB
 * expression</a> to a JSON object. The only supported variable is <code>$$NOW</code>. The following
 * operators are not supported: <code>$filter</code>, <code>$map</code>, <code>$reduce</code>,
 * <code>$indexOfBytes</code>, <code>$strLenBytes</code>, <code>$substrBytes</code>, <code>$toDate
 * </code>, <code>$toObjectId</code> and the date expression operators. Accumulators and variable
 * expression operators are also not supported. Only the aggregation variable <code>$$NOW</code> is
 * supported.
 *
 * @author Werner Donn\u00e9
 * @since 1.2
 */
public class Expression {
  private static final String ABS = "$abs";
  private static final String ADD = "$add";
  private static final String ALL_ELEMENTS_TRUE = "$allElementsTrue";
  private static final String AND = "$and";
  private static final String ANY_ELEMENTS_TRUE = "$anyElementsTrue";
  private static final String ARRAY_ELEM_AT = "$arrayElemAt";
  private static final String ARRAY_TO_OBJECT = "$arrayToObject";
  private static final String ACOS = "$acos";
  private static final String ACOSH = "$acosh";
  private static final String ASIN = "$asin";
  private static final String ASINH = "$asinh";
  private static final String ATAN = "$atan";
  private static final String ATANH = "$atanh";
  private static final String ATAN2 = "$atan2";
  private static final String CEIL = "$ceil";
  private static final String CMP = "$cmp";
  private static final String CONCAT = "$concat";
  private static final String CONCAT_ARRAYS = "$concatArrays";
  private static final String COND = "$cond";
  private static final String CONVERT = "$convert";
  private static final String COS = "$cos";
  private static final String DEGREES_TO_RADIANS = "$degreesToRadians";
  private static final String DIVIDE = "$divide";
  private static final String EQ = "$eq";
  private static final String EXP = "$exp";
  private static final String FLOOR = "$floor";
  private static final String GT = "$gt";
  private static final String GTE = "$gte";
  private static final String IF_NULL = "$ifNull";
  private static final String IN = "$in";
  private static final String INDEX_OF_ARRAY = "$indexOfArray";
  private static final String INDEX_OF_CP = "$indexOfCP";
  private static final String IS_ARRAY = "$isArray";
  private static final String LITERAL = "$literal";
  private static final String LN = "$ln";
  private static final String LOG = "$log";
  private static final String LOG_10 = "$log10";
  private static final String LT = "$lt";
  private static final String LTE = "$lte";
  private static final String LTRIM = "$ltrim";
  private static final String MERGE_OBJECTS = "$mergeObjects";
  private static final String MOD = "$mod";
  private static final String MULTIPLY = "$multiply";
  private static final String NE = "$ne";
  private static final String NOT = "$not";
  private static final String NOW = "$$NOW";
  private static final String OBJECT_TO_ARRAY = "$objectToArray";
  private static final String OR = "$or";
  private static final String POW = "$pow";
  private static final String RADIANS_TO_DEGREES = "$radiansToDegrees";
  private static final String RANGE = "$range";
  private static final String REGEX_FIND = "$regexFind";
  private static final String REGEX_FIND_ALL = "$regexFindAll";
  private static final String REGEX_MATCH = "$regexMatch";
  private static final String REVERSE_ARRAY = "$reverseArray";
  private static final String ROUND = "$round";
  private static final String RTRIM = "$rtrim";
  private static final String SET_DIFFERENCE = "$setDifference";
  private static final String SET_EQUALS = "$setEquals";
  private static final String SET_INTERSECTION = "$setIntersection";
  private static final String SET_IS_SUBSET = "$setIsSubset";
  private static final String SET_UNION = "$setUnion";
  private static final String SIN = "$sin";
  private static final String SIZE = "$size";
  private static final String SLICE = "$slice";
  private static final String SPLIT = "$split";
  private static final String SQRT = "$sqrt";
  private static final String STR_LEN_CP = "$strLenCP";
  private static final String STRCASECMP = "$strcasecmp";
  private static final String SUBSTR_CP = "$substrCP";
  private static final String SUBTRACT = "$subtract";
  private static final String SWITCH = "$switch";
  private static final String TAN = "$tan";
  private static final String TO_BOOL = "$toBool";
  private static final String TO_DECIMAL = "$toDecimal";
  private static final String TO_DOUBLE = "$toDouble";
  private static final String TO_INT = "$toInt";
  private static final String TO_LONG = "$toLong";
  private static final String TO_LOWER = "$toLower";
  private static final String TO_STRING = "$toString";
  private static final String TO_UPPER = "$toUpper";
  private static final String TRIM = "$trim";
  private static final String TRUNC = "$trunc";
  private static final String TYPE = "$type";
  private static final String ZIP = "$zip";
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>>
      ARITHMETIC =
          map(
              pair(ABS, Arithmetic::abs),
              pair(ADD, Arithmetic::add),
              pair(CEIL, Arithmetic::ceil),
              pair(DIVIDE, Arithmetic::divide),
              pair(EXP, Arithmetic::exp),
              pair(FLOOR, Arithmetic::floor),
              pair(LN, Arithmetic::ln),
              pair(LOG, Arithmetic::log),
              pair(LOG_10, Arithmetic::log10),
              pair(MOD, Arithmetic::mod),
              pair(MULTIPLY, Arithmetic::multiply),
              pair(POW, Arithmetic::pow),
              pair(ROUND, Arithmetic::round),
              pair(SQRT, Arithmetic::sqrt),
              pair(SUBTRACT, Arithmetic::subtract),
              pair(TRUNC, Arithmetic::trunc));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>> ARRAYS =
      map(
          pair(ARRAY_ELEM_AT, Arrays::arrayElemAt),
          pair(ARRAY_TO_OBJECT, Arrays::arrayToObject),
          pair(CONCAT_ARRAYS, Arrays::concatArrays),
          pair(IN, Arrays::in),
          pair(INDEX_OF_ARRAY, Arrays::indexOfArray),
          pair(IS_ARRAY, Arrays::isArray),
          pair(OBJECT_TO_ARRAY, Arrays::objectToArray),
          pair(RANGE, Arrays::range),
          pair(REVERSE_ARRAY, Arrays::reverseArray),
          pair(SIZE, Arrays::size),
          pair(SLICE, Arrays::slice));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>> BOOLEANS =
      map(pair(AND, Booleans::and), pair(NOT, Booleans::not), pair(OR, Booleans::or));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>>
      CONDITIONAL =
          map(
              pair(COND, Conditional::cond),
              pair(IF_NULL, Conditional::ifNull),
              pair(SWITCH, Conditional::switchFunction));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>>
      RELATIONAL =
          map(
              pair(EQ, asFunction(Relational::eq)),
              pair(GT, asFunction(Relational::gt)),
              pair(GTE, asFunction(Relational::gte)),
              pair(LT, asFunction(Relational::lt)),
              pair(LTE, asFunction(Relational::lte)),
              pair(NE, asFunction(Relational::ne)));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>> SETS =
      map(
          pair(ALL_ELEMENTS_TRUE, Sets::allElementsTrue),
          pair(ANY_ELEMENTS_TRUE, Sets::anyElementsTrue),
          pair(SET_DIFFERENCE, Sets::setDifference),
          pair(SET_EQUALS, Sets::setEquals),
          pair(SET_INTERSECTION, Sets::setIntersection),
          pair(SET_IS_SUBSET, Sets::setIsSubset),
          pair(SET_UNION, Sets::setUnion));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>> STRINGS =
      map(
          pair(CONCAT, Strings::concat),
          pair(INDEX_OF_CP, Strings::indexOfCP),
          pair(LTRIM, Strings::ltrim),
          pair(REGEX_FIND, Strings::regexFind),
          pair(REGEX_FIND_ALL, Strings::regexFindAll),
          pair(REGEX_MATCH, Strings::regexMatch),
          pair(RTRIM, Strings::rtrim),
          pair(SPLIT, Strings::split),
          pair(STR_LEN_CP, Strings::strLenCP),
          pair(STRCASECMP, Strings::strcasecmp),
          pair(SUBSTR_CP, Strings::substrCP),
          pair(TO_LOWER, Strings::toLower),
          pair(TO_UPPER, Strings::toUpper),
          pair(TRIM, Strings::trim));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>>
      TRIGONOMETRY =
          map(
              pair(ACOS, Trigonometry::acos),
              pair(ACOSH, Trigonometry::acosh),
              pair(ASIN, Trigonometry::asin),
              pair(ASINH, Trigonometry::asinh),
              pair(ATAN, Trigonometry::atan),
              pair(ATANH, Trigonometry::atanh),
              pair(ATAN2, Trigonometry::atan2),
              pair(COS, Trigonometry::cos),
              pair(DEGREES_TO_RADIANS, Trigonometry::degreesToRadians),
              pair(RADIANS_TO_DEGREES, Trigonometry::radiansToDegrees),
              pair(SIN, Trigonometry::sin),
              pair(TAN, Trigonometry::tan));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>> TYPES =
      map(
          pair(CONVERT, Types::convert),
          pair(TO_BOOL, Types::toBool),
          pair(TO_DECIMAL, Types::toDecimal),
          pair(TO_DOUBLE, Types::toDouble),
          pair(TO_INT, Types::toInt),
          pair(TO_LONG, Types::toLong),
          pair(TO_STRING, Types::toString),
          pair(TYPE, Types::type));
  private static final Map<String, Function<JsonValue, Function<JsonObject, JsonValue>>> FUNCTIONS =
      merge(
          ARITHMETIC,
          ARRAYS,
          BOOLEANS,
          CONDITIONAL,
          RELATIONAL,
          SETS,
          STRINGS,
          TRIGONOMETRY,
          TYPES,
          map(
              pair(CMP, Cmp::cmp),
              pair(LITERAL, Expression::literal),
              pair(MERGE_OBJECTS, Expression::mergeObjects),
              pair(ZIP, Zip::zip)));

  private Expression() {}

  static Optional<List<JsonValue>> applyFunctions(
      final List<Function<JsonObject, JsonValue>> functions, final JsonObject json) {
    return applyFunctions(functions, json, fncs -> true);
  }

  static Optional<List<JsonValue>> applyFunctions(
      final List<Function<JsonObject, JsonValue>> functions,
      final JsonObject json,
      final Predicate<List<Function<JsonObject, JsonValue>>> condition) {
    return Optional.ofNullable(functions)
        .filter(condition)
        .map(fncs -> fncs.stream().map(f -> f == null ? null : f.apply(json)).collect(toList()));
  }

  static Optional<List<JsonValue>> applyFunctionsNum(
      final List<Function<JsonObject, JsonValue>> functions, final JsonObject json, final int num) {
    return applyFunctions(functions, json, fncs -> fncs.size() == num);
  }

  static Function<JsonObject, JsonValue> arraysOperator(
      final JsonValue value, final Function<List<JsonArray>, JsonValue> op) {
    return multipleOperator(value, op, JsonUtil::isArray, JsonValue::asJsonArray);
  }

  static Function<JsonObject, JsonValue> arraysOperatorTwo(
      final JsonValue value, final BiFunction<JsonArray, JsonArray, JsonValue> op) {
    return multipleOperator(
        value,
        arrays -> arrays.size() == 2 ? op.apply(arrays.get(0), arrays.get(1)) : null,
        JsonUtil::isArray,
        JsonValue::asJsonArray);
  }

  static Function<JsonObject, JsonValue> bigMath(
      final JsonValue value, final UnaryOperator<BigDecimal> op) {
    return math(value, JsonNumber::bigDecimalValue, op);
  }

  static Function<JsonObject, JsonValue> bigMathTwo(
      final JsonValue value, final BinaryOperator<BigDecimal> op, final boolean optional) {
    return mathTwo(value, JsonNumber::bigDecimalValue, op, optional);
  }

  private static Function<JsonObject, JsonValue> function(final String key, final JsonValue value) {
    return Optional.ofNullable(FUNCTIONS.get(key)).map(fn -> fn.apply(value)).orElse(json -> NULL);
  }

  private static Function<JsonObject, JsonValue> function(final JsonObject expression) {
    return key(expression)
        .map(key -> function(key, expression.getValue("/" + key)))
        .orElse(json -> NULL);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @return The function, which is stateless.
   * @since 1.2
   */
  public static Function<JsonObject, JsonValue> function(final JsonValue expression) {
    return isObject(expression) ? function(expression.asJsonObject()) : value(expression);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @return The function, which is stateless.
   * @since 1.2
   */
  public static Function<JsonObject, JsonValue> function(final Bson expression) {
    return function(fromBson(toBsonDocument(expression)));
  }

  static List<Function<JsonObject, JsonValue>> functions(final JsonValue value) {
    return JsonUtil.isArray(value)
        ? value.asJsonArray().stream().map(Expression::function).collect(toList())
        : null;
  }

  static int getInteger(final List<JsonValue> values, final int index) {
    return asInt(values.get(index));
  }

  static String getString(final List<JsonValue> values, final int index) {
    return asString(values.get(index)).getString();
  }

  static boolean isFalse(final JsonValue value) {
    return value.equals(FALSE)
        || value.equals(NULL)
        || (isNumber(value) && asNumber(value).intValue() == 0);
  }

  static boolean isScalar(final JsonValue value) {
    return !(value instanceof JsonStructure);
  }

  private static Function<JsonObject, JsonValue> literal(final JsonValue value) {
    return json -> value;
  }

  @SuppressWarnings("java:S4276") // Not compatible.
  static Function<JsonObject, JsonValue> math(
      final JsonValue value, final UnaryOperator<Double> op) {
    return math(value, JsonNumber::doubleValue, op);
  }

  private static <T> Function<JsonObject, JsonValue> math(
      final JsonValue value, final Function<JsonNumber, T> toValue, final UnaryOperator<T> op) {
    final Function<JsonObject, JsonValue> function = function(value);

    return json ->
        numeric(function, json).map(toValue).map(op).map(JsonUtil::createValue).orElse(NULL);
  }

  @SuppressWarnings("java:S4276") // Not compatible.
  static Function<JsonObject, JsonValue> mathTwo(
      final JsonValue value, final BinaryOperator<Double> op, final boolean optional) {
    return mathTwo(value, JsonNumber::doubleValue, op, optional);
  }

  private static <T> Function<JsonObject, JsonValue> mathTwo(
      final JsonValue value,
      final Function<JsonNumber, T> toValue,
      final BinaryOperator<T> op,
      final boolean optional) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json, fncs -> (fncs.size() == 1 && optional) || fncs.size() == 2)
            .filter(values -> values.stream().allMatch(JsonUtil::isNumber))
            .map(
                values ->
                    op.apply(
                        toValue.apply(asNumber(values.get(0))),
                        values.size() == 2 ? toValue.apply(asNumber(values.get(1))) : null))
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  static <T> Optional<T> member(
      final JsonValue value, final String name, final Function<JsonValue, T> extract) {
    return Optional.of(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .flatMap(o -> getValue(o, "/" + name))
        .map(extract);
  }

  static Function<JsonObject, JsonValue> memberFunction(final JsonValue value, final String name) {
    return member(value, name, Expression::function).orElse(null);
  }

  static List<Function<JsonObject, JsonValue>> memberFunctions(
      final JsonValue value, final String name) {
    return member(value, name, Expression::functions).orElse(null);
  }

  private static Function<JsonObject, JsonValue> mergeObjects(final JsonValue value) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json)
            .filter(values -> values.stream().allMatch(JsonUtil::isObject))
            .map(Expression::mergeObjects)
            .orElse(NULL);
  }

  private static JsonValue mergeObjects(final List<JsonValue> values) {
    return values.stream()
        .map(JsonValue::asJsonObject)
        .reduce(
            createObjectBuilder(),
            (b, o) -> copy(o, b, (k, ob) -> !ob.get(k).equals(NULL)),
            (b1, b2) -> b1)
        .build();
  }

  private static <T> Function<JsonObject, JsonValue> multipleOperator(
      final JsonValue value,
      final Function<List<T>, JsonValue> op,
      final Predicate<JsonValue> predicate,
      final Function<JsonValue, T> map) {
    final List<Function<JsonObject, JsonValue>> functions = functions(value);

    return json ->
        applyFunctions(functions, json)
            .map(values -> values.stream().filter(predicate).map(map).collect(toList()))
            .filter(list -> list.size() == functions.size())
            .map(op)
            .orElse(NULL);
  }

  private static Optional<JsonNumber> numeric(
      final Function<JsonObject, JsonValue> function, final JsonObject json) {
    return Optional.ofNullable(function)
        .map(f -> f.apply(json))
        .filter(JsonUtil::isNumber)
        .map(JsonUtil::asNumber);
  }

  static Function<JsonObject, JsonValue> stringsOperator(
      final JsonValue value, final Function<List<String>, JsonValue> op) {
    return multipleOperator(value, op, JsonUtil::isString, v -> asString(v).getString());
  }

  private static Function<JsonObject, JsonValue> value(final JsonValue value) {
    return json ->
        Optional.of(value)
            .filter(JsonUtil::isString)
            .map(JsonUtil::asString)
            .map(JsonString::getString)
            .flatMap(
                s ->
                    Or.<JsonValue>tryWith(
                            () -> s.equals(NOW) ? createValue(now().toString()) : null)
                        .or(
                            () ->
                                s.startsWith("$")
                                    ? getValue(json, toJsonPointer(s.substring(1))).orElse(null)
                                    : null)
                        .get())
            .orElse(value);
  }
}
