package net.pincette.mongo;

import static java.time.Instant.now;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static net.pincette.json.JsonUtil.asInt;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.copy;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isNumber;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.mongo.BsonUtil.fromBson;
import static net.pincette.mongo.BsonUtil.toBsonDocument;
import static net.pincette.mongo.Relational.asFunction;
import static net.pincette.mongo.Util.LOGGER;
import static net.pincette.mongo.Util.key;
import static net.pincette.mongo.Util.toArray;
import static net.pincette.mongo.Util.unwrapTrace;
import static net.pincette.util.Collections.map;
import static net.pincette.util.Collections.merge;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.json.Jq;
import net.pincette.json.Jslt;
import net.pincette.json.JsonUtil;
import net.pincette.util.Pair;
import org.bson.conversions.Bson;

/**
 * This class lets you apply a <a
 * href="https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#expressions">MongoDB
 * expression</a> to a JSON object. The following operators are not supported: <code>$indexOfBytes
 * </code>, <code>$strLenBytes</code>, <code>$substrBytes</code>, <code>$toDate</code>, <code>
 * $toObjectId</code> and the date expression operators. Accumulators are also not supported. Only
 * the aggregation variables <code>$$NOW</code> and <code>$$ROOT</code> are supported. The extension
 * variable <code>$$TODAY</code> returns the current date as an ISO 8601 string.
 *
 * <p>The <code>$unescape</code> extension operator converts key names that start with "#$" to "$".
 * This way an expression can be escaped from implementation generation.
 *
 * <p>The expression of the <code>$jq</code> extension operator should be an object with the fields
 * <code>input</code> and <code>script</code>. The former is an expression that should produce a
 * JSON value. If it is absent <code>$$ROOT</code> will be assumed. The latter is a reference to a
 * JQ script. If the value starts with "resource:" then it is treated as a resource in the class
 * path. Otherwise, it is a filename or a script. The result of the expression will be a JSON value.
 * If the expression is just a string, then it will be handled as a script value.
 *
 * <p>The expression of the <code>$jslt</code> extension operator should be an object with the
 * fields <code>input</code> and <code>script</code>. The former is an expression that should
 * produce a JSON value. If it is absent <code>$$ROOT</code> will be assumed. The latter is a
 * reference to a JSLT script. If the value starts with "resource:" then it is treated as a resource
 * in the class path. Otherwise, it is a filename or a script. The result of the expression will be
 * a JSON value. If the expression is just a string, then it will be handled as a script value.
 *
 * <p>The <code>$sort</code> extension operator receives an object with the mandatory field <code>
 * input</code>, which should be an expression that yields an array. The optional field <code>
 * direction</code> can have the values "asc" or "desc", the former being the default. The optional
 * field <code>paths</code> is a list of field paths. When it is present, only object values in the
 * array are considered. They will be sorted hierarchically with the values extracted with the
 * paths.
 *
 * <p>The operator <code>$elemMatch</code>, which is normally part of the MongoDB query language, is
 * available in another form. Its value must be an array with two elements. The first element is an
 * expression that yields an array and the second is an <code>$elemMatch</code> specification. If
 * behaves as the <a
 * href="https://docs.mongodb.com/manual/reference/operator/projection/elemMatch/">projection
 * variant</a> of the operator.
 *
 * <p>If you wrap an expression in the <code>$trace</code> operator then tracing will be done for it
 * in the logger "net.pincette.mongo.expressions" at level <code>INFO</code>.
 *
 * <p>The extension operators <code>$fromEpochMillis</code>, <code>$fromEpochNanos</code>, <code>
 * $fromEpochSeconds</code>, <code>$toEpochMillis</code>, <code>$toEpochNanos</code>, <code>
 * $toEpochSeconds</code>, <code>$toDate</code>, <code>$toDay</code>, <code>$toMonth</code> and
 * <code>$toYear</code> work with ISO8601 timestamps.
 *
 * @author Werner Donné
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
  private static final String BASE64_DECODE = "$base64Decode";
  private static final String BASE64_ENCODE = "$base64Encode";
  private static final String CEIL = "$ceil";
  private static final String CMP = "$cmp";
  private static final String CONCAT = "$concat";
  private static final String CONCAT_ARRAYS = "$concatArrays";
  private static final String COND = "$cond";
  private static final String CONVERT = "$convert";
  private static final String COS = "$cos";
  private static final String DEGREES_TO_RADIANS = "$degreesToRadians";
  private static final String DIVIDE = "$divide";
  private static final String ELEM_MATCH = "$elemMatch";
  private static final String EQ = "$eq";
  private static final String EXP = "$exp";
  private static final String FILTER = "$filter";
  private static final String FIRST = "$first";
  private static final String FLOOR = "$floor";
  private static final String FROM_EPOCH_MILLIS = "$fromEpochMillis";
  private static final String FROM_EPOCH_NANOS = "$fromEpochNanos";
  private static final String FROM_EPOCH_SECONDS = "$fromEpochSeconds";
  private static final String GT = "$gt";
  private static final String GTE = "$gte";
  private static final String IF_NULL = "$ifNull";
  private static final String IN = "$in";
  private static final String IN_FIELD = "in";
  private static final String INDEX_OF_ARRAY = "$indexOfArray";
  private static final String INDEX_OF_CP = "$indexOfCP";
  private static final String INPUT = "input";
  private static final String IS_ARRAY = "$isArray";
  private static final String JQ = "$jq";
  private static final String JSLT = "$jslt";
  private static final String JSON_TO_STRING = "$jsonToString";
  private static final String LAST = "$last";
  private static final String LET = "$let";
  private static final String LITERAL = "$literal";
  private static final String LN = "$ln";
  private static final String LOG = "$log";
  private static final String LOG_10 = "$log10";
  private static final String LT = "$lt";
  private static final String LTE = "$lte";
  private static final String LTRIM = "$ltrim";
  private static final String MAP = "$map";
  private static final String MERGE_OBJECTS = "$mergeObjects";
  private static final String MOD = "$mod";
  private static final String MULTIPLY = "$multiply";
  private static final String NE = "$ne";
  private static final String NOT = "$not";
  private static final String NOW = "$$NOW";
  private static final String OBJECT_TO_ARRAY = "$objectToArray";
  private static final String OR = "$or";
  private static final String SORT = "$sort";
  private static final String POW = "$pow";
  private static final String RADIANS_TO_DEGREES = "$radiansToDegrees";
  private static final String RANGE = "$range";
  private static final String REDUCE = "$reduce";
  private static final String REGEX_FIND = "$regexFind";
  private static final String REGEX_FIND_ALL = "$regexFindAll";
  private static final String REGEX_MATCH = "$regexMatch";
  private static final String REVERSE_ARRAY = "$reverseArray";
  private static final String REPLACE_ALL = "$replaceAll";
  private static final String REPLACE_ONE = "$replaceOne";
  private static final String ROOT = "$$ROOT";
  private static final String ROUND = "$round";
  private static final String RTRIM = "$rtrim";
  private static final String SCRIPT = "script";
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
  private static final String STRING_TO_JSON = "$stringToJson";
  private static final String SUBSTR_CP = "$substrCP";
  private static final String SUBTRACT = "$subtract";
  private static final String SWITCH = "$switch";
  private static final String TAN = "$tan";
  private static final String TO_BOOL = "$toBool";
  private static final String TO_DATE = "$toDate";
  private static final String TO_DAY = "$toDay";
  private static final String TO_DECIMAL = "$toDecimal";
  private static final String TO_DOUBLE = "$toDouble";
  private static final String TO_EPOCH_MILLIS = "$toEpochMillis";
  private static final String TO_EPOCH_NANOS = "$toEpochNanos";
  private static final String TO_EPOCH_SECONDS = "$toEpochSeconds";
  private static final String TO_INT = "$toInt";
  private static final String TO_LONG = "$toLong";
  private static final String TO_LOWER = "$toLower";
  private static final String TO_MONTH = "$toMonth";
  private static final String TO_STRING = "$toString";
  private static final String TO_UPPER = "$toUpper";
  private static final String TO_YEAR = "$toYear";
  private static final String TODAY = "$$TODAY";
  private static final String TRIM = "$trim";
  private static final String TRUNC = "$trunc";
  private static final String TYPE = "$type";
  private static final String UNESCAPE = "$unescape";
  private static final String URI_DECODE = "$uriDecode";
  private static final String URI_ENCODE = "$uriEncode";
  private static final String VARS = "vars";
  private static final String ZIP = "$zip";
  private static final Map<String, Operator> ARITHMETIC =
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
  private static final Map<String, Operator> ARRAYS =
      map(
          pair(ARRAY_ELEM_AT, Arrays::arrayElemAt),
          pair(ARRAY_TO_OBJECT, Arrays::arrayToObject),
          pair(CONCAT_ARRAYS, Arrays::concatArrays),
          pair(ELEM_MATCH, Arrays::elemMatch),
          pair(FILTER, Arrays::filter),
          pair(FIRST, Arrays::first),
          pair(IN, Arrays::in),
          pair(INDEX_OF_ARRAY, Arrays::indexOfArray),
          pair(IS_ARRAY, Arrays::isArray),
          pair(LAST, Arrays::last),
          pair(MAP, Arrays::mapOp),
          pair(OBJECT_TO_ARRAY, Arrays::objectToArray),
          pair(RANGE, Arrays::range),
          pair(REDUCE, Arrays::reduce),
          pair(REVERSE_ARRAY, Arrays::reverseArray),
          pair(SIZE, Arrays::size),
          pair(SLICE, Arrays::slice),
          pair(SORT, Arrays::sort));
  private static final Map<String, Operator> BOOLEANS =
      map(pair(AND, Booleans::and), pair(NOT, Booleans::not), pair(OR, Booleans::or));
  private static final Map<String, Operator> CONDITIONAL =
      map(
          pair(COND, Conditional::cond),
          pair(IF_NULL, Conditional::ifNull),
          pair(SWITCH, Conditional::switchFunction));
  private static final Map<String, Operator> ISO8601 =
      map(
          pair(FROM_EPOCH_MILLIS, Iso8601::fromEpochMillis),
          pair(FROM_EPOCH_NANOS, Iso8601::fromEpochNanos),
          pair(FROM_EPOCH_SECONDS, Iso8601::fromEpochSeconds),
          pair(TO_DATE, Iso8601::toDate),
          pair(TO_DAY, Iso8601::toDay),
          pair(TO_EPOCH_MILLIS, Iso8601::toEpochMillis),
          pair(TO_EPOCH_NANOS, Iso8601::toEpochNanos),
          pair(TO_EPOCH_SECONDS, Iso8601::toEpochSeconds),
          pair(TO_MONTH, Iso8601::toMonth),
          pair(TO_YEAR, Iso8601::toYear));
  private static final Map<String, Operator> RELATIONAL =
      map(
          pair(EQ, asFunction(Relational::eq)),
          pair(GT, asFunction(Relational::gt)),
          pair(GTE, asFunction(Relational::gte)),
          pair(LT, asFunction(Relational::lt)),
          pair(LTE, asFunction(Relational::lte)),
          pair(NE, asFunction(Relational::ne)));
  private static final Map<String, Operator> SETS =
      map(
          pair(ALL_ELEMENTS_TRUE, Sets::allElementsTrue),
          pair(ANY_ELEMENTS_TRUE, Sets::anyElementsTrue),
          pair(SET_DIFFERENCE, Sets::setDifference),
          pair(SET_EQUALS, Sets::setEquals),
          pair(SET_INTERSECTION, Sets::setIntersection),
          pair(SET_IS_SUBSET, Sets::setIsSubset),
          pair(SET_UNION, Sets::setUnion));
  private static final Map<String, Operator> STRINGS =
      map(
          pair(BASE64_DECODE, Strings::base64Decode),
          pair(BASE64_ENCODE, Strings::base64Encode),
          pair(CONCAT, Strings::concat),
          pair(INDEX_OF_CP, Strings::indexOfCP),
          pair(JSON_TO_STRING, Strings::jsonToString),
          pair(LTRIM, Strings::ltrim),
          pair(REGEX_FIND, Strings::regexFind),
          pair(REGEX_FIND_ALL, Strings::regexFindAll),
          pair(REGEX_MATCH, Strings::regexMatch),
          pair(REPLACE_ALL, Strings::replaceAll),
          pair(REPLACE_ONE, Strings::replaceOne),
          pair(RTRIM, Strings::rtrim),
          pair(SPLIT, Strings::split),
          pair(STR_LEN_CP, Strings::strLenCP),
          pair(STRCASECMP, Strings::strcasecmp),
          pair(STRING_TO_JSON, Strings::stringToJson),
          pair(SUBSTR_CP, Strings::substrCP),
          pair(TO_LOWER, Strings::toLower),
          pair(TO_UPPER, Strings::toUpper),
          pair(TRIM, Strings::trim),
          pair(URI_DECODE, Strings::uriDecode),
          pair(URI_ENCODE, Strings::uriEncode));
  private static final Map<String, Operator> TRIGONOMETRY =
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
  private static final Map<String, Operator> TYPES =
      map(
          pair(CONVERT, Types::convert),
          pair(TO_BOOL, Types::toBool),
          pair(TO_DECIMAL, Types::toDecimal),
          pair(TO_DOUBLE, Types::toDouble),
          pair(TO_INT, Types::toInt),
          pair(TO_LONG, Types::toLong),
          pair(TO_STRING, Types::toString),
          pair(TYPE, Types::type));
  private static final Map<String, Operator> OPERATORS =
      merge(
          ARITHMETIC,
          ARRAYS,
          BOOLEANS,
          CONDITIONAL,
          ISO8601,
          RELATIONAL,
          SETS,
          STRINGS,
          TRIGONOMETRY,
          TYPES,
          map(
              pair(CMP, Cmp::cmp),
              pair(JQ, Expression::jq),
              pair(JSLT, Expression::jslt),
              pair(LET, Expression::let),
              pair(LITERAL, (v, f) -> literal(v)),
              pair(MERGE_OBJECTS, Expression::mergeObjects),
              pair(UNESCAPE, Expression::unescape),
              pair(ZIP, Zip::zip)));

  private Expression() {}

  static Optional<List<JsonValue>> applyImplementations(
      final List<Implementation> implementations,
      final JsonObject json,
      final Map<String, JsonValue> variables) {
    return applyImplementations(implementations, json, variables, fncs -> true);
  }

  static Optional<List<JsonValue>> applyImplementations(
      final List<Implementation> implementations,
      final JsonObject json,
      final Map<String, JsonValue> variables,
      final Predicate<List<Implementation>> condition) {
    return ofNullable(implementations)
        .filter(condition)
        .map(fncs -> fncs.stream().map(f -> f == null ? null : f.apply(json, variables)).toList());
  }

  static Optional<List<JsonValue>> applyImplementationsNum(
      final List<Implementation> functions,
      final JsonObject json,
      final Map<String, JsonValue> variables,
      final int num) {
    return applyImplementations(functions, json, variables, fncs -> fncs.size() == num);
  }

  private static Map<String, JsonValue> applyVariables(
      final JsonObject json,
      final Map<String, JsonValue> variables,
      final Map<String, Implementation> implementations) {
    return merge(
        variables,
        implementations.entrySet().stream()
            .collect(toMap(Entry::getKey, e -> e.getValue().apply(json, variables))));
  }

  static Implementation arraysOperator(
      final JsonValue value,
      final Function<List<JsonArray>, JsonValue> op,
      final Features features) {
    return multipleOperator(value, op, JsonUtil::isArray, JsonValue::asJsonArray, features);
  }

  static Implementation arraysOperatorTwo(
      final JsonValue value,
      final BiFunction<JsonArray, JsonArray, JsonValue> op,
      final Features features) {
    return multipleOperator(
        value,
        arrays -> arrays.size() == 2 ? op.apply(arrays.getFirst(), arrays.get(1)) : null,
        JsonUtil::isArray,
        JsonValue::asJsonArray,
        features);
  }

  /**
   * Returns a result when it is an array.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asArray(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isArray);
  }

  /**
   * Returns a result when it is a boolean.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asBoolean(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isBoolean);
  }

  /**
   * Returns a result when it is a double number.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asDouble(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isDouble);
  }

  /**
   * Returns a result when it is an UTC instant.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asInstant(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isInstant);
  }

  /**
   * Returns a result when it is a long number.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asLong(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isLong);
  }

  /**
   * Returns a result when it is a JSON number.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asNumeric(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isNumber);
  }

  /**
   * Returns a result when it is an object.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asObject(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isObject);
  }

  /**
   * Returns a result when it is a string.
   *
   * @param implementation the original implementation.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional asString(final Implementation implementation) {
    return implementationOptional(implementation, JsonUtil::isString);
  }

  static Implementation bigMath(
      final JsonValue value, final UnaryOperator<BigDecimal> op, final Features features) {
    return math(value, JsonNumber::bigDecimalValue, op, features);
  }

  static Implementation bigMathTwo(
      final JsonValue value,
      final BinaryOperator<BigDecimal> op,
      final boolean optional,
      final Features features) {
    return mathTwo(value, JsonNumber::bigDecimalValue, op, optional, features);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @return The function, which is stateless.
   * @since 1.2
   */
  public static Function<JsonObject, JsonValue> function(final JsonValue expression) {
    return function(expression, null, null);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @param features extra features. It may be <code>null</code>.
   * @return The function, which is stateless.
   * @since 2.0
   */
  public static Function<JsonObject, JsonValue> function(
      final JsonValue expression, final Features features) {
    return function(expression, null, features);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @param variables external variables. It may be <code>null</code>.
   * @return The function, which is stateless.
   * @since 1.3
   */
  public static Function<JsonObject, JsonValue> function(
      final JsonValue expression, final Map<String, JsonValue> variables) {
    return function(expression, variables, null);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @param variables external variables. It may be <code>null</code>.
   * @param features extra features. It may be <code>null</code>.
   * @return The function, which is stateless.
   * @since 2.0
   */
  public static Function<JsonObject, JsonValue> function(
      final JsonValue expression, final Map<String, JsonValue> variables, final Features features) {
    final Implementation implementation = implementation(expression, features);
    final Map<String, JsonValue> vars = stripDollars(variables != null ? variables : emptyMap());

    return json -> implementation.apply(json, vars);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @return The function, which is stateless.
   * @since 1.2
   */
  public static Function<JsonObject, JsonValue> function(final Bson expression) {
    return function(expression, null, null);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @param features extra features. It may be <code>null</code>.
   * @return The function, which is stateless.
   * @since 2.0
   */
  public static Function<JsonObject, JsonValue> function(
      final Bson expression, final Features features) {
    return function(expression, null, features);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @param variables external variables.
   * @return The function, which is stateless.
   * @since 1.3
   */
  public static Function<JsonObject, JsonValue> function(
      final Bson expression, final Map<String, JsonValue> variables) {
    return function(expression, variables, null);
  }

  /**
   * Constructs a function with <code>expression</code>.
   *
   * @param expression the MongoDB expression.
   * @param variables external variables.
   * @param features extra features. It may be <code>null</code>.
   * @return The function, which is stateless.
   * @since 2.0
   */
  public static Function<JsonObject, JsonValue> function(
      final Bson expression, final Map<String, JsonValue> variables, final Features features) {
    return function(fromBson(toBsonDocument(expression)), variables, features);
  }

  static int getInteger(final List<JsonValue> values, final int index) {
    return asInt(values.get(index));
  }

  static String getString(final List<JsonValue> values, final int index) {
    return JsonUtil.asString(values.get(index)).getString();
  }

  private static Optional<JsonValue> getVariable(
      final Map<String, JsonValue> variables, final String name) {
    return ofNullable(
        variables.get(
            Optional.of(name.indexOf('.'))
                .filter(i -> i != -1)
                .map(i -> name.substring(0, i))
                .orElse(name)));
  }

  private static Optional<Implementation> implementation(
      final String key, final JsonValue expression, final Features features) {
    return tryWith(() -> OPERATORS.get(key))
        .or(
            () ->
                ofNullable(features)
                    .map(f -> f.expressionExtensions)
                    .map(e -> e.get(key))
                    .orElse(null))
        .get()
        .map(fn -> fn.apply(expression, features));
  }

  /**
   * Extension developers can use this to delegate implementation generation to subexpressions.
   *
   * @param expression the given expression.
   * @param features extra features. It may be <code>null</code>.
   * @return The generated implementation.
   * @since 2.0
   */
  public static Implementation implementation(final JsonValue expression, final Features features) {
    final Pair<JsonValue, Boolean> unwrapped = unwrapTrace(expression);
    final Supplier<Implementation> tryArray =
        () ->
            isArray(unwrapped.first)
                ? implementation(unwrapped.first.asJsonArray(), features)
                : value(unwrapped.first);

    return wrapLogging(
        isObject(unwrapped.first)
            ? implementation(unwrapped.first.asJsonObject(), features)
            : tryArray.get(),
        unwrapped.first,
        Boolean.TRUE.equals(unwrapped.second) ? INFO : FINEST);
  }

  private static Implementation implementation(
      final JsonObject expression, final Features features) {
    return key(expression)
        .flatMap(key -> implementation(key, expression.getValue("/" + key), features))
        .orElseGet(() -> recursiveImplementation(expression, features));
  }

  private static Implementation implementation(
      final JsonArray expression, final Features features) {
    final List<Implementation> implementations =
        expression.stream().map(expr -> implementation(expr, features)).toList();

    return (json, vars) ->
        implementations.stream()
            .reduce(createArrayBuilder(), (b, i) -> b.add(i.apply(json, vars)), (b1, b2) -> b1)
            .build();
  }

  static List<Implementation> implementations(final JsonValue expression, final Features features) {
    return isArray(expression)
        ? expression.asJsonArray().stream().map(expr -> implementation(expr, features)).toList()
        : null;
  }

  static boolean isFalse(final JsonValue value) {
    return value.equals(FALSE)
        || value.equals(NULL)
        || (isNumber(value) && asNumber(value).intValue() == 0);
  }

  static boolean isScalar(final JsonValue value) {
    return !(value instanceof JsonStructure);
  }

  private static Implementation jq(final JsonValue value, final Features features) {
    return script(value, jqScript(value, features), features);
  }

  private static UnaryOperator<JsonValue> jqScript(final JsonValue value, final Features features) {
    return scriptValue(value)
        .map(
            s ->
                Jq.transformerValue(
                    new Jq.Context(Jq.tryReader(s))
                        .withModuleLoader(features != null ? features.jqModuleLoader : null)))
        .orElse(null);
  }

  private static Implementation jslt(final JsonValue value, final Features features) {
    return script(value, jsltScript(value, features), features);
  }

  private static UnaryOperator<JsonValue> jsltScript(
      final JsonValue value, final Features features) {
    return scriptValue(value)
        .map(
            s ->
                Jslt.transformerValue(
                    new Jslt.Context(Jslt.tryReader(s))
                        .withResolver(features != null ? features.jsltResolver : null)
                        .withFunctions(features != null ? features.customJsltFunctions : null)))
        .orElse(null);
  }

  private static Implementation let(final JsonValue value, final Features features) {
    final Implementation in = memberFunction(value, IN_FIELD, features);
    final Map<String, Implementation> variables = variables(value, features);

    return (json, vars) ->
        ofNullable(in).map(i -> i.apply(json, applyVariables(json, vars, variables))).orElse(NULL);
  }

  private static Implementation literal(final JsonValue value) {
    return (json, vars) -> value;
  }

  private static JsonValue log(
      final JsonValue expression,
      final JsonObject json,
      final Map<String, JsonValue> variables,
      final JsonValue result,
      final Level level) {
    LOGGER.log(
        level,
        () ->
            "Expression:\n"
                + string(expression)
                + "\n\nWith:\n"
                + string(json)
                + "\n\nVariables:\n"
                + variables(variables)
                + "\n\nYields: "
                + string(result)
                + "\n\n");

    return result;
  }

  /**
   * If you set the log level to FINEST, you will get a trace.
   *
   * @return The logger with the name "net.pincette.mongo.expression".
   * @since 1.3
   */
  public static Logger logger() {
    return LOGGER;
  }

  @SuppressWarnings("java:S4276") // Not compatible.
  static Implementation math(
      final JsonValue value, final UnaryOperator<Double> op, final Features features) {
    return math(value, JsonNumber::doubleValue, op, features);
  }

  private static <T> Implementation math(
      final JsonValue value,
      final Function<JsonNumber, T> toValue,
      final UnaryOperator<T> op,
      final Features features) {
    final ImplementationOptional implementation = asNumeric(implementation(value, features));

    return (json, vars) ->
        implementation
            .apply(json, vars)
            .map(JsonUtil::asNumber)
            .map(toValue)
            .map(op)
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  @SuppressWarnings("java:S4276") // Not compatible.
  static Implementation mathTwo(
      final JsonValue value,
      final BinaryOperator<Double> op,
      final boolean optional,
      final Features features) {
    return mathTwo(value, JsonNumber::doubleValue, op, optional, features);
  }

  private static <T> Implementation mathTwo(
      final JsonValue value,
      final Function<JsonNumber, T> toValue,
      final BinaryOperator<T> op,
      final boolean optional,
      final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(
                implementations,
                json,
                vars,
                fncs -> (fncs.size() == 1 && optional) || fncs.size() == 2)
            .filter(values -> values.stream().allMatch(JsonUtil::isNumber))
            .map(
                values ->
                    op.apply(
                        toValue.apply(asNumber(values.getFirst())),
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

  /**
   * If <code>expression</code> is an object, which has a subexpression called <code>name</code>,
   * then this method returns the implementation of the subexpression.
   *
   * @param expression the given expression.
   * @param name the name of the subexpression.
   * @param features extra features. It may be <code>null</code>.
   * @return The implementation of the subexpression. If <code>expression</code> is not an object or
   *     if the subexpression is invalid <code>null</code> will be returned.
   * @since 2.0
   */
  public static Implementation memberFunction(
      final JsonValue expression, final String name, final Features features) {
    return member(expression, name, expr -> implementation(expr, features)).orElse(null);
  }

  /**
   * If <code>expression</code> is an object, which has a subexpression called <code>name</code>,
   * and if the subexpression is actually an array of subexpressions, then this method returns the
   * implementation of the subexpressions.
   *
   * @param expression the given expression.
   * @param name the name of the subexpressions.
   * @param features extra features. It may be <code>null</code>.
   * @return The implementation of the subexpressions. If <code>expression</code> is not an object
   *     or if the subexpressions are invalid <code>null</code> will be returned.
   * @since 2.0
   */
  public static List<Implementation> memberFunctions(
      final JsonValue expression, final String name, final Features features) {
    return member(expression, name, expr -> implementations(expr, features)).orElse(null);
  }

  private static Implementation mergeObjects(final JsonValue value, final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(implementations, json, vars)
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

  private static <T> Implementation multipleOperator(
      final JsonValue value,
      final Function<List<T>, JsonValue> op,
      final Predicate<JsonValue> predicate,
      final Function<JsonValue, T> map,
      final Features features) {
    final List<Implementation> implementations = implementations(value, features);

    return (json, vars) ->
        applyImplementations(implementations, json, vars)
            .map(values -> values.stream().filter(predicate).map(map).toList())
            .filter(
                list -> {
                  assert implementations != null;
                  return list.size() == implementations.size();
                })
            .map(op)
            .orElse(NULL);
  }

  /**
   * Adds a condition to the result of an implementation.
   *
   * @param implementation the original implementation.
   * @param condition the condition.
   * @return The optional implementation.
   * @since 4.3.0
   */
  public static ImplementationOptional implementationOptional(
      final Implementation implementation, final Predicate<JsonValue> condition) {
    return (json, variables) ->
        Optional.of(implementation.apply(json, variables)).filter(condition);
  }

  private static Implementation recursiveImplementation(
      final JsonObject expression, final Features features) {
    final Map<String, Implementation> implementations =
        expression.entrySet().stream()
            .collect(toMap(Entry::getKey, e -> implementation(e.getValue(), features)));

    return (json, vars) ->
        implementations.entrySet().stream()
            .reduce(
                createObjectBuilder(),
                (b, e) -> b.add(e.getKey(), e.getValue().apply(json, vars)),
                (b1, b2) -> b1)
            .build();
  }

  /**
   * Replaces all <code>variables</code> in <code>expression</code>. Variable names start with "$$".
   *
   * @param expression the given expression.
   * @param variables maps variable names to JSON values.
   * @return The new expression.
   * @since 1.3.2
   */
  public static JsonValue replaceVariables(
      final JsonValue expression, final Map<String, JsonValue> variables) {
    return switch (expression.getValueType()) {
      case ARRAY -> replaceVariables(expression.asJsonArray(), variables);
      case OBJECT -> replaceVariables(expression.asJsonObject(), variables);
      case STRING -> replaceVariables(JsonUtil.asString(expression), variables);
      default -> expression;
    };
  }

  private static JsonValue replaceVariables(
      final JsonString expression, final Map<String, JsonValue> variables) {
    return Optional.of(expression.getString())
        .flatMap(expr -> getVariable(variables, expr).map(value -> pair(expr, value)))
        .map(pair -> value(pair.second, pair.first.substring(2)))
        .orElse(expression);
  }

  private static JsonValue replaceVariables(
      final JsonArray expressions, final Map<String, JsonValue> variables) {
    return toArray(expressions.stream().map(v -> replaceVariables(v, variables)));
  }

  private static JsonValue replaceVariables(
      final JsonObject expressions, final Map<String, JsonValue> variables) {
    return expressions.entrySet().stream()
        .reduce(
            createObjectBuilder(),
            (b, e) -> b.add(e.getKey(), replaceVariables(e.getValue(), variables)),
            (b1, b2) -> b1)
        .build();
  }

  private static Implementation script(
      final JsonValue value, final UnaryOperator<JsonValue> op, final Features features) {
    final Implementation input = scriptInput(value, features);

    return (json, vars) ->
        input != null && op != null
            ? Optional.of(input.apply(json, vars))
                .filter(JsonUtil::isObject)
                .map(JsonValue::asJsonObject)
                .map(op)
                .map(JsonUtil::createValue)
                .orElse(NULL)
            : NULL;
  }

  private static Implementation scriptInput(final JsonValue value, final Features features) {
    return tryWith(() -> memberFunction(value, INPUT, features))
        .or(() -> implementation(createValue(ROOT), features))
        .get()
        .orElse(null);
  }

  private static Optional<String> scriptValue(final JsonValue value) {
    return tryWith(() -> member(value, SCRIPT, v -> stringValue(v).orElse(null)).orElse(null))
        .or(() -> stringValue(value).orElse(null))
        .get();
  }

  static Implementation stringsOperator(
      final JsonValue value, final Function<List<String>, JsonValue> op, final Features features) {
    return multipleOperator(
        value, op, JsonUtil::isString, v -> JsonUtil.asString(v).getString(), features);
  }

  private static Map<String, JsonValue> stripDollars(final Map<String, JsonValue> variables) {
    return variables.entrySet().stream()
        .collect(toMap(e -> stripDollars(e.getKey()), Entry::getValue));
  }

  private static String stripDollars(final String name) {
    return name.startsWith("$$") ? name.substring(2) : name;
  }

  private static Implementation unescape(final JsonValue value, final Features features) {
    final Implementation implementation = implementation(value, features);

    return (json, vars) -> unescapeKeys(implementation.apply(json, vars));
  }

  private static String unescapeKey(final String key) {
    return key.startsWith("#$") ? key.substring(1) : key;
  }

  private static JsonValue unescapeKeys(final JsonValue value) {
    return switch (value.getValueType()) {
      case ARRAY -> unescapeKeys(value.asJsonArray());
      case OBJECT -> unescapeKeys(value.asJsonObject());
      default -> value;
    };
  }

  private static JsonValue unescapeKeys(final JsonArray array) {
    return toArray(array.stream().map(Expression::unescapeKeys));
  }

  private static JsonValue unescapeKeys(final JsonObject object) {
    return object.entrySet().stream()
        .reduce(
            createObjectBuilder(),
            (b, e) -> b.add(unescapeKey(e.getKey()), unescapeKeys(e.getValue())),
            (b1, b2) -> b1)
        .build();
  }

  private static Implementation value(final JsonValue value) {
    return (json, variables) ->
        stringValue(value).flatMap(s -> value(json, s, variables)).orElse(value);
  }

  private static Optional<JsonValue> value(
      final JsonObject json, final String value, final Map<String, JsonValue> variables) {
    return tryWith(() -> value.equals(NOW) ? createValue(now().toString()) : null)
        .or(
            (Supplier<JsonValue>)
                () -> value.equals(TODAY) ? createValue(LocalDate.now().toString()) : null)
        .or((Supplier<JsonValue>) () -> value.equals(ROOT) ? json : null)
        .or(
            (Supplier<JsonValue>)
                () -> value.startsWith("$$") ? value(variables, value.substring(2)) : null)
        .or(
            (Supplier<JsonValue>)
                () ->
                    value.startsWith("$")
                        ? getValue(json, toJsonPointer(value.substring(1))).orElse(NULL)
                        : null)
        .get();
  }

  private static JsonValue value(final Map<String, JsonValue> variables, final String variable) {
    final Pair<String, String> name = variableName(variable);

    return value(variables.get(name.first), name);
  }

  static JsonValue value(final JsonValue value, final String variable) {
    return value(value, variableName(variable));
  }

  private static JsonValue value(final JsonValue value, final Pair<String, String> variable) {
    return ofNullable(value)
        .filter(v -> variable.second == null || isObject(v))
        .map(
            v ->
                variable.second != null
                    ? getValue(v.asJsonObject(), toJsonPointer(variable.second)).orElse(NULL)
                    : value)
        .orElse(NULL);
  }

  private static Pair<String, String> variableName(final String variable) {
    return Optional.of(variable.indexOf('.'))
        .filter(i -> i != -1)
        .map(i -> pair(variable.substring(0, i), variable.substring(i + 1)))
        .orElseGet(() -> pair(variable, null));
  }

  private static Map<String, Implementation> variables(
      final JsonValue value, final Features features) {
    return Optional.of(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .map(json -> json.getJsonObject(VARS))
        .map(JsonObject::entrySet)
        .map(Set::stream)
        .map(
            stream ->
                stream.collect(toMap(Entry::getKey, e -> implementation(e.getValue(), features))))
        .orElseGet(Collections::emptyMap);
  }

  private static String variables(final Map<String, JsonValue> variables) {
    return variables.entrySet().stream()
        .map(e -> e.getKey() + ": " + string(e.getValue()))
        .collect(joining("\n"));
  }

  private static Implementation wrapLogging(
      final Implementation implementation, final JsonValue expression, final Level level) {
    return (json, vars) -> log(expression, json, vars, implementation.apply(json, vars), level);
  }
}
