package net.pincette.mongo;

import static java.util.Optional.ofNullable;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static net.pincette.json.JsonUtil.add;
import static net.pincette.json.JsonUtil.arrayValue;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getObject;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.objectValue;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.json.Transform.nopTransformer;
import static net.pincette.json.Transform.transform;
import static net.pincette.mongo.Match.predicate;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.computeIfAbsent;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.getParent;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.json.Transform.JsonEntry;
import net.pincette.json.Transform.Transformer;
import net.pincette.util.Pair;

/**
 * With this class you can generate a validator based on a specification in JSON. Use one instance
 * to generate several validators. This way common specifications are loaded and compiled only once.
 * A specification is a JSON document with the following fields, all of which are optional.
 *
 * <dl>
 *   <dt>conditions
 *   <dd>An array of MongoDB query expressions. which can have the standard <code>$comment</code>
 *       operator and the extension operator <code>$code</code>. The former is for documentation
 *       purposes and the latter will be returned in the errors array in the <code>code</code> field
 *       when the expression returns <code>false</code>. If the value of a field in the expression
 *       has an object with only the field <code>ref</code> as its value, then the conditions
 *       referred to by <code>ref</code> will be applied to the field. If it is an array with such a
 *       ref object, then the value is first checked to be an array and the referred conditions will
 *       be applied to the objects in the array. When a condition is an expression for a field it
 *       will be tested only when that field is present and the condition is not a <code>$exists
 *       </code> expression.
 *   <dt>description
 *   <dd>A short string describing the specification.
 *   <dt>include
 *   <dd>An array of source strings, which can be filenames or class path resources, which start
 *       with "resource:". They refer to other specifications, the contents of which is merged with
 *       the including specification. All conditions in the included specifications will be in force
 *       and all of their macros are available. The including specification may redefine a macro,
 *       but it will only affect itself and other specifications it is included in.
 *   <dt>macros
 *   <dd>A JSON object where each field represents a macro. If a field is called <code>myfield
 *       </code> then you can use it in conditions and other macros as <code>_myfield_</code>. Such
 *       an occurrence will be replaced with the value in the macro object.
 *   <dt>title
 *   <dd>The title of the specification.
 * </dl>
 *
 * <p>This is an example:
 *
 * <pre>{
 *   "title": "The structure of the select slot command.",
 *   "include": [
 *     "resource:/validators/jes/command.json",
 *     "resource:/validators/active_location.json"
 *   ],
 *   "conditions": [
 *     {
 *       "_command": "Select"
 *     },
 *     {
 *       "_type": "invitations-slot",
 *       "$code": "TYPE"
 *     },
 *     {
 *       "_state.free": {
 *         "$eq": true
 *       },
 *       "$comment": "The slot should be free.",
 *       "$code": "FREE"
 *     },
 *     {
 *       "invitation": {
 *         "$exists": true
 *       },
 *       "$code": "REQUIRED"
 *     },
 *     {
 *       "$expr": {
 *         "$eq": [
 *           "$invitation.larsGroup.clbId",
 *           "$_state.clbId"
 *         ]
 *       },
 *       "$code": "SAME_CLB"
 *     },
 *     {
 *       "_jwt": {
 *         "ref": "../../jwt.json"
 *       }
 *     },
 *     {
 *       "clbs": [
 *         {
 *           "ref": "clb.json"
 *         }
 *       ]
 *     }
 *   ]
 * }</pre>
 *
 * @author Werner Donné
 * @since 1.3
 * @see Match
 * @see Expression
 */
public class Validator {
  private static final String CODE = "$code";
  private static final String COMMENT = "$comment";
  private static final String CONDITIONS = "conditions";
  private static final String DESCRIPTION = "description";
  private static final String ERROR_CODE = "code";
  private static final String ERROR_LOCATION = "location";
  private static final String EXISTS = "$exists";
  private static final String INCLUDE = "include";
  private static final String LOCATION = "$location";
  private static final String MACROS = "macros";
  private static final String REF = "ref";
  private static final String TITLE = "title";
  private static final Set<String> REMOVE = set(COMMENT, DESCRIPTION, TITLE);
  private static final Transformer REMOVER =
      new Transformer(
          entry -> getLastSegment(entry.path, ".").map(REMOVE::contains).orElse(false),
          entry -> Optional.empty());
  private final Map<JsonObject, Condition> conditionCache = new HashMap<>();
  private final Features features;
  private Resolver resolver;

  public Validator() {
    this(null);
  }

  /**
   * Creates a validator with extra features for the underlying MongoDB query language.
   *
   * @param features the extra features. It may be <code>null</code>.
   * @since 2.0
   */
  public Validator(final Features features) {
    this(features, null);
  }

  /**
   * Creates a validator with extra features for the underlying MongoDB query language and a
   * resolver.
   *
   * @param features the extra features. It may be <code>null</code>.
   * @param resolver the resolver, which resolves includes and <code>ref</code> fields. It may be
   *     <code>null</code>.
   * @since 2.2
   */
  public Validator(final Features features, final Resolver resolver) {
    this.features = features;
    setResolver(resolver != null ? resolver : new SourceResolver()::resolve);
  }

  private static Transformer arrayExpander(final JsonObject macros) {
    return new Transformer(
        entry -> isArray(entry.value),
        entry ->
            Optional.of(new JsonEntry(entry.path, expandArray(entry.value.asJsonArray(), macros))));
  }

  private static JsonArrayBuilder combineConditions(
      final JsonObject json, final Stream<JsonValue> included) {
    return concat(
            included,
            ofNullable(json.getJsonArray(CONDITIONS)).stream().flatMap(Collection::stream))
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1);
  }

  private static JsonObject combineMacros(final JsonObject json, final JsonObject includedMacros) {
    return add(
        includedMacros,
        ofNullable(json.getJsonObject(MACROS))
            .map(
                macros ->
                    !includedMacros.isEmpty()
                        ? transform(macros, expanders(includedMacros))
                        : macros)
            .orElseGet(JsonUtil::emptyObject));
  }

  private static Condition condition(
      final JsonObject condition, final String code, final Features features) {
    final String field = getField(condition);
    final String conditionPath = ofNullable(field).map(JsonUtil::toJsonPointer).orElse("");
    final boolean isExists = field != null && isExists(condition.get(field));
    final Predicate<JsonObject> test = predicate(strip(condition), features);

    return (json, path) ->
        Optional.of(testObject(json, path, conditionPath))
                .map(
                    j ->
                        (!conditionPath.isEmpty() && !parentExists(json, path))
                            || (conditionPath.isEmpty() && getValue(json, path).isEmpty())
                            || (!conditionPath.isEmpty()
                                && !isExists
                                && getValue(j, conditionPath).isEmpty())
                            || test.test(testObject(json, path, conditionPath)))
                .orElse(false)
            ? empty()
            : of(createError(path, code));
  }

  private static JsonObject createError(final String path, final String code) {
    return create(JsonUtil::createObjectBuilder)
        .update(b -> b.add(ERROR_LOCATION, path))
        .updateIf(() -> ofNullable(code), (b, c) -> b.add(ERROR_CODE, c))
        .build()
        .build();
  }

  private static JsonValue expand(final JsonValue value, final JsonObject macros) {
    return getMacroRef(value).flatMap(ref -> getValue(macros, "/" + ref)).orElse(value);
  }

  private static JsonArray expandArray(final JsonArray array, final JsonObject macros) {
    return array.stream()
        .map(value -> isMacroRef(value) ? expand(value, macros) : value)
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  private static Transformer expander(final JsonObject macros) {
    return new Transformer(
        entry -> isMacroRef(entry.value),
        entry -> Optional.of(new JsonEntry(entry.path, expand(entry.value, macros))));
  }

  private static Transformer expanders(final JsonObject macros) {
    return !macros.isEmpty() ? arrayExpander(macros).thenApply(expander(macros)) : nopTransformer();
  }

  private static String getField(final JsonObject condition) {
    return condition.keySet().stream().filter(k -> !k.startsWith("$")).findFirst().orElse(null);
  }

  private static Optional<String> getMacroRef(final JsonValue value) {
    return stringValue(value)
        .filter(s -> s.startsWith("_") && s.endsWith("_"))
        .map(s -> s.substring(1, s.length() - 1));
  }

  private static String getPath(final JsonObject condition, final String parentPath) {
    return parentPath
        + tryWith(() -> getField(condition))
            .or(() -> condition.getString(LOCATION, null))
            .get()
            .map(JsonUtil::toJsonPointer)
            .orElse("");
  }

  private static Optional<String> getRef(final JsonValue value) {
    return objectValue(value)
        .filter(json -> hasOnlyThisKey(json, REF))
        .map(json -> json.getString(REF, null))
        .or(() -> getRefInArray(value));
  }

  private static Optional<String> getRefInArray(final JsonValue value) {
    return arrayValue(value)
        .filter(a -> a.size() == 1)
        .map(List::getFirst)
        .flatMap(Validator::getRef);
  }

  private static boolean hasOnlyThisKey(final JsonObject json, final String key) {
    return Optional.of(json.keySet())
        .map(keys -> keys.size() == 1 && keys.iterator().next().equals(key))
        .orElse(false);
  }

  private static JsonObject include(
      final JsonObject json, final Resolver resolver, final String context) {
    final Pair<Stream<JsonValue>, JsonObject> included = loadIncluded(json, resolver, context);

    return createObjectBuilder(json)
        .remove(INCLUDE)
        .remove(MACROS)
        .add(CONDITIONS, combineConditions(json, included.first))
        .add(MACROS, combineMacros(json, included.second))
        .build();
  }

  private static boolean isConditions(final JsonValue value) {
    return isObject(value) && value.asJsonObject().containsKey(CONDITIONS);
  }

  private static boolean isExists(final JsonValue expression) {
    return Optional.of(expression)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(json -> json.containsKey(EXISTS))
        .isPresent();
  }

  private static boolean isMacroRef(final JsonValue value) {
    return getMacroRef(value).isPresent();
  }

  private static boolean isRef(final JsonValue value) {
    return getRef(value).isPresent();
  }

  private static Pair<Stream<JsonValue>, JsonObject> loadIncluded(
      final JsonObject json, final Resolver resolver, final String context) {
    return getStrings(json, INCLUDE)
        .map(s -> resolver.apply(s, context))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(resolved -> resolve(resolved.specification, resolver, resolved.source))
        .map(included -> pair(included.getJsonArray(CONDITIONS).stream(), macros(included)))
        .reduce(
            pair(empty(), emptyObject()),
            (r, p) -> pair(concat(r.first, p.first), add(r.second, p.second)),
            (r1, r2) -> r1);
  }

  private static JsonObject macros(final JsonObject specification) {
    return ofNullable(specification.getJsonObject(MACROS)).orElseGet(JsonUtil::emptyObject);
  }

  private static boolean parentExists(final JsonValue json, final String path) {
    final String parent = getParent(path, "/");

    return parent.equals("/")
        || !isObject(json)
        || getValue(json.asJsonObject(), parent).isPresent();
  }

  private static Optional<String> parentPath(final String path, final String conditionPath) {
    return Optional.of(path.lastIndexOf(conditionPath))
        .filter(i -> i != -1)
        .map(i -> path.substring(0, i));
  }

  private static Transformer refResolver(final Resolver resolver, final String context) {
    return new Transformer(
        entry -> isRef(entry.value),
        entry ->
            getRef(entry.value)
                .flatMap(ref -> resolver.apply(ref, context))
                .map(resolved -> resolve(resolved.specification, resolver, resolved.source))
                .map(
                    resolved ->
                        isArray(entry.value)
                            ? createArrayBuilder().add(resolved).build()
                            : resolved)
                .map(resolved -> new JsonEntry(entry.path, resolved)));
  }

  /**
   * When a validation specification includes other specifications they are resolved recursively.
   *
   * @param specification the validation specification.
   * @param resolver the resolver for resolving all inclusions and references.
   * @param context the context to resolve relative references with. It may be <code>null</code>.
   * @return the resolved validation specification.
   * @since 2.2
   */
  public static JsonObject resolve(
      final JsonObject specification, final Resolver resolver, final String context) {
    final JsonObject json = include(specification, resolver, context);

    return transform(
        json, expanders(macros(json)).thenApply(refResolver(resolver, context)).thenApply(REMOVER));
  }

  private static JsonObject strip(final JsonObject condition) {
    return createObjectBuilder(condition).remove(LOCATION).remove(CODE).build();
  }

  private static JsonObject testObject(
      final JsonObject json, final String path, final String conditionPath) {
    return parentPath(path, conditionPath)
        .filter(p -> !p.isEmpty())
        .flatMap(p -> getObject(json, p))
        .orElse(json);
  }

  private Condition condition(final JsonObject condition) {
    final Condition c =
        computeIfAbsent(conditionCache, condition, k -> generateCondition(k, features));

    return (json, path) -> c.apply(json, getPath(condition, path));
  }

  private Condition conditions(final String field, final JsonValue value) {
    return isConditions(value)
        ? conditionsObject(value.asJsonObject())
        : conditionArray(field, value.asJsonArray());
  }

  private Condition conditionArray(final String field, final JsonArray array) {
    final Condition conditions = conditionsObject(array.getFirst().asJsonObject());

    return (json, path) ->
        getArray(json, toJsonPointer(field))
            .map(
                values ->
                    zip(values.stream(), rangeExclusive(0, values.size()))
                        .flatMap(
                            pair ->
                                isObject(pair.first)
                                    ? conditions.apply(json, path + "/" + pair.second)
                                    : empty()))
            .orElseGet(Stream::empty);
  }

  private Condition conditionsObject(final JsonObject conditions) {
    final List<Condition> c = getObjects(conditions, CONDITIONS).map(this::condition).toList();

    return (json, path) -> c.stream().flatMap(condition -> condition.apply(json, path));
  }

  private Condition generateCondition(final JsonObject condition, final Features features) {
    return ofNullable(getField(condition))
        .flatMap(
            field -> getValue(condition, toJsonPointer(field)).map(value -> pair(field, value)))
        .filter(pair -> isConditions(pair.second) || isArray(pair.second))
        .map(pair -> conditions(pair.first, pair.second))
        .orElseGet(() -> condition(condition, condition.getString(CODE, null), features));
  }

  /**
   * Loads and resolves a validation specification.
   *
   * @param source either a filename or a class path resource, in which case <code>source</code>
   *     should start with "resource:".
   * @return The specification.
   * @since 1.4.1
   */
  public JsonObject load(final String source) {
    return load(source, (String) null);
  }

  /**
   * Loads and resolves a validation specification.
   *
   * @param source either a filename or a class path resource, in which case <code>source</code>
   *     should start with "resource:".
   * @param baseDirectory the directory to resolve relative file references with. It may be <code>
   *     null</code>.
   * @return The specification.
   * @since 1.4.1
   */
  public JsonObject load(final String source, final File baseDirectory) {
    return load(source, ofNullable(baseDirectory).map(File::getAbsolutePath).orElse(null));
  }

  /**
   * Loads and resolves a validation specification.
   *
   * @param source either a filename or a class path resource, in which case <code>source</code>
   *     should start with "resource:".
   * @param context the context to resolve relative references with. It may be <code>null</code>.
   * @return The specification.
   * @since 2.2
   */
  public JsonObject load(final String source, final String context) {
    return resolver
        .apply(source, context)
        .map(resolved -> resolve(resolved.specification, resolver, resolved.source))
        .orElse(null);
  }

  /**
   * When a validation specification includes other specifications they are resolved recursively.
   *
   * @param specification the validation specification.
   * @return the resolved validation specification.
   * @since 1.4.1
   */
  public JsonObject resolve(final JsonObject specification) {
    return resolve(specification, (String) null);
  }

  /**
   * When a validation specification includes other specifications they are resolved recursively.
   *
   * @param specification the validation specification.
   * @param baseDirectory the directory to resolve relative file references with. It may be <code>
   *     null</code>.
   * @return the resolved validation specification.
   * @since 1.4.1
   */
  public JsonObject resolve(final JsonObject specification, final File baseDirectory) {
    return resolve(
        specification, ofNullable(baseDirectory).map(File::getAbsolutePath).orElse(null));
  }

  /**
   * When a validation specification includes other specifications they are resolved recursively.
   *
   * @param specification the validation specification.
   * @param context the context to resolve relative references with. It may be <code>null</code>.
   * @return the resolved validation specification.
   * @since 2.2
   */
  public JsonObject resolve(final JsonObject specification, final String context) {
    return resolve(specification, resolver, context);
  }

  /**
   * Sets the include resolver.
   *
   * @param resolver the given resolver.
   * @since 2.2.4
   */
  public void setResolver(final Resolver resolver) {
    this.resolver = resolver;
  }

  /**
   * Generates a validator with either a filename or a class path resource, in which case <code>
   * source</code> should start with "resource:".
   *
   * @param source the validation specification.
   * @return An array with the fields <code>location</code>, which is a JSON pointer, and <code>code
   *     </code>, which is the value of the <code>$code</code> field in the specification.
   * @since 1.3
   */
  public Function<JsonObject, JsonArray> validator(final String source) {
    return validator(load(source));
  }

  /**
   * Generates a validator with the specification. It will be resolved first.
   *
   * @param specification the validation specification.
   * @return An array with the fields <code>location</code>, which is a JSON pointer, and <code>code
   *     </code>, which is the value of the <code>$code</code> field in the specification.
   * @since 1.4.1
   */
  public Function<JsonObject, JsonArray> validator(final JsonObject specification) {
    final Condition conditions = conditionsObject(resolve(specification));

    return json ->
        conditions
            .apply(json, "")
            .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
            .build();
  }

  private interface Condition extends BiFunction<JsonObject, String, Stream<JsonValue>> {}

  /**
   * The first argument of the function is the source that has to be resolved. The second argument
   * is the context in which the resolution happens. The context may be the outer context that is
   * passed through the API or the resolved source of another specification. The function returns
   * the resolved specification.
   *
   * @since 2.2
   */
  public interface Resolver extends BiFunction<String, String, Optional<Resolved>> {}

  /**
   * This represents a specification with its resolved source.
   *
   * @since 2.2
   */
  public static class Resolved {
    final String source;
    final JsonObject specification;

    public Resolved(final JsonObject specification, final String source) {
      this.specification = specification;
      this.source = source;
    }
  }
}
