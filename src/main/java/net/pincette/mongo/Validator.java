package net.pincette.mongo;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.empty;
import static java.util.stream.Stream.of;
import static net.pincette.json.JsonUtil.add;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.emptyObject;
import static net.pincette.json.JsonUtil.getArray;
import static net.pincette.json.JsonUtil.getObjects;
import static net.pincette.json.JsonUtil.getStrings;
import static net.pincette.json.JsonUtil.getValue;
import static net.pincette.json.JsonUtil.isArray;
import static net.pincette.json.JsonUtil.isObject;
import static net.pincette.json.JsonUtil.toJsonPointer;
import static net.pincette.json.Transform.transform;
import static net.pincette.mongo.Match.predicate;
import static net.pincette.mongo.Match.predicateValue;
import static net.pincette.util.Builder.create;
import static net.pincette.util.Collections.set;
import static net.pincette.util.Or.tryWith;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.StreamUtil.rangeExclusive;
import static net.pincette.util.StreamUtil.zip;
import static net.pincette.util.Util.getLastSegment;
import static net.pincette.util.Util.getParent;
import static net.pincette.util.Util.resolveFile;
import static net.pincette.util.Util.to;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
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
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import net.pincette.function.SideEffect;
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
 *       when the expression returns <code>false</code>.
 *   <dt>description
 *   <dd>A short string describing the specification.
 *   <dt>include
 *   <dd>An array of source strings, which can be filenames or class path resources, which start
 *       with "resource:". They refer to other specifications, the contents of which is merged with
 *       the including specification. All conditions in the included specifications will be in force
 *       and all of their macros are available. The including specification may redefine a macro,
 *       but it will only affect itself and other specifications it is included in.
 *   <dt>macros
 *   <dd>A JSON object where each field represents a macro. It a field is called <code>myfield
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
 *     }
 *   ]
 * }</pre>
 *
 * @author Werner Donn\u00e9
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
  private static final String EXPAND = "expand";
  private static final String INCLUDE = "include";
  private static final String JSLT_PATH = "$jslt.script";
  private static final String LOCATION = "$location";
  private static final String MACROS = "macros";
  private static final String REF = "ref";
  private static final String RESOURCE = "resource:";
  private static final String TITLE = "title";
  private static final String WITH = "with";
  private static final Set<String> REMOVE = set(COMMENT, DESCRIPTION, TITLE);
  private static final Transformer REMOVER =
      new Transformer(
          entry -> getLastSegment(entry.path, "\\.").map(REMOVE::contains).orElse(false),
          entry -> Optional.empty());
  private final Map<JsonObject, Condition> conditionCache = new HashMap<>();
  private final Features features;
  private final Map<String, JsonObject> loaded = new HashMap<>();

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
    this.features = features;
  }

  private static Transformer arrayExpander(final JsonObject macros) {
    return new Transformer(
        entry -> isArray(entry.value),
        entry ->
            Optional.of(
                new JsonEntry(
                    entry.path,
                    entry.value.asJsonArray().stream()
                        .map(value -> isMacroRef(value) ? expand(value, macros) : value)
                        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
                        .build())));
  }

  private static JsonArrayBuilder combineConditions(
      final JsonObject json, final Stream<JsonValue> included) {
    return concat(
            included,
            ofNullable(json.getJsonArray(CONDITIONS))
                .map(JsonArray::stream)
                .orElseGet(Stream::empty))
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1);
  }

  private static JsonObject combineMacros(final JsonObject json, final JsonObject included) {
    return add(
        included,
        ofNullable(json.getJsonObject(MACROS))
            .map(macros -> transform(macros, expanders(included)))
            .orElseGet(JsonUtil::emptyObject));
  }

  private static Condition condition(
      final JsonObject condition, final String code, final Features features) {
    final String field = getField(condition);
    final boolean isExists = field != null && isExists(condition.get(field));
    final Predicate<JsonObject> predicateObject = predicate(strip(condition), features);
    final Predicate<JsonValue> predicateValue = predicateValue(strip(condition), features);
    final Predicate<JsonValue> test =
        json ->
            isObject(json) ? predicateObject.test(json.asJsonObject()) : predicateValue.test(json);

    return (json, path) ->
        !parentExists(json, path)
                || (field != null
                    && !isExists
                    && !getValue(json.asJsonObject(), toJsonPointer(field)).isPresent())
                || test.test(json)
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

  private static Transformer expand(final JsonObject json) {
    return ofNullable(json.getJsonObject(MACROS))
        .map(macros -> with(macros).thenApply(expanders(macros)))
        .orElseGet(() -> with(emptyObject()));
  }

  private static JsonValue expand(final JsonValue value, final JsonObject macros) {
    return getMacroRef(value).flatMap(ref -> getValue(macros, "/" + ref)).orElse(value);
  }

  private static Transformer expander(final JsonObject macros) {
    return new Transformer(
        entry -> isMacroRef(entry.value),
        entry -> Optional.of(new JsonEntry(entry.path, expand(entry.value, macros))));
  }

  private static Transformer expanders(final JsonObject macros) {
    return arrayExpander(macros).thenApply(expander(macros));
  }

  private static String getField(final JsonObject condition) {
    return condition.keySet().stream().filter(k -> !k.startsWith("$")).findFirst().orElse(null);
  }

  private static Optional<JsonObject> getInstruction(final JsonValue value, final String name) {
    return Optional.of(value)
        .filter(JsonUtil::isObject)
        .map(JsonValue::asJsonObject)
        .filter(
            json ->
                Optional.of(json.keySet())
                    .map(keys -> keys.size() == 1 && keys.iterator().next().equals(name))
                    .orElse(false));
  }

  private static Optional<String> getMacroRef(final JsonValue value) {
    return Optional.of(value)
        .filter(JsonUtil::isString)
        .map(JsonUtil::asString)
        .map(JsonString::getString)
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

  private static Optional<JsonObject> getRef(final JsonValue value) {
    return getInstruction(value, REF);
  }

  private static Optional<JsonObject> getWith(final JsonValue value) {
    return getInstruction(value, WITH);
  }

  private static JsonObject include(
      final JsonObject json, final Map<String, JsonObject> loaded, final File baseDirectory) {
    final Pair<Stream<JsonValue>, JsonObject> included = loadIncluded(json, loaded, baseDirectory);

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

  private static boolean isJsltPath(final String path) {
    return path.endsWith(JSLT_PATH);
  }

  private static boolean isMacroRef(final JsonValue value) {
    return getMacroRef(value).isPresent();
  }

  private static boolean isRef(final JsonValue value) {
    return getRef(value).isPresent();
  }

  private static boolean isResource(final String ref) {
    return ref.startsWith(RESOURCE);
  }

  private static boolean isWith(final JsonValue value) {
    return getWith(value).isPresent();
  }

  private static Transformer jsltResolver(final File baseDirectory) {
    return new Transformer(
        entry -> isJsltPath(entry.path),
        entry ->
            resolveFile(baseDirectory, asString(entry.value).getString())
                .map(File::getAbsolutePath)
                .map(file -> new JsonEntry(entry.path, createValue(file))));
  }

  private static JsonObject load(
      final Reader reader, final Map<String, JsonObject> loaded, final File baseDirectory) {
    return resolve(
        tryToGetWithRethrow(() -> createReader(reader), JsonReader::readObject).orElse(null),
        loaded,
        baseDirectory);
  }

  private static JsonObject load(
      final InputStream in, final Map<String, JsonObject> loaded, final File baseDirectory) {
    return load(new InputStreamReader(in, UTF_8), loaded, baseDirectory);
  }

  private static JsonObject load(final File file, final Map<String, JsonObject> loaded) {
    return load(
        tryToGetRethrow(() -> new FileInputStream(file)).orElse(null),
        loaded,
        file.getParentFile());
  }

  private static JsonObject load(final String resource, final Map<String, JsonObject> loaded) {
    return load(Validator.class.getResourceAsStream(resource), loaded, null);
  }

  private static Pair<Stream<JsonValue>, JsonObject> loadIncluded(
      final JsonObject json, final Map<String, JsonObject> loaded, final File baseDirectory) {
    return getStrings(json, INCLUDE)
        .map(ref -> loadRef(ref, loaded, baseDirectory))
        .map(
            ref ->
                pair(
                    ref.getJsonArray(CONDITIONS).stream(),
                    ofNullable(ref.getJsonObject(MACROS)).orElseGet(JsonUtil::emptyObject)))
        .reduce(
            pair(empty(), emptyObject()),
            (r, p) -> pair(concat(r.first, p.first), add(r.second, p.second)),
            (r1, r2) -> r1);
  }

  private static JsonObject loadRef(
      final String ref, final Map<String, JsonObject> loaded, final File baseDirectory) {
    final String realRef =
        isResource(ref) || baseDirectory == null
            ? ref
            : new File(baseDirectory, ref).getAbsolutePath();

    return ofNullable(loaded.get(realRef))
        .orElseGet(
            () ->
                SideEffect.<JsonObject>run(
                        () ->
                            loaded.put(
                                realRef,
                                isResource(ref)
                                    ? load(resourcePath(ref), loaded)
                                    : load(new File(realRef), loaded)))
                    .andThenGet(() -> loaded.get(realRef)));
  }

  private static boolean parentExists(final JsonValue json, final String path) {
    final String parent = getParent(path, "/");

    return parent.equals("/")
        || !isObject(json)
        || getValue(json.asJsonObject(), parent).isPresent();
  }

  private static Transformer refResolver(
      final Map<String, JsonObject> loaded, final File baseDirectory) {
    return new Transformer(
        entry -> isRef(entry.value),
        entry ->
            Optional.of(entry.value.asJsonObject().getString(REF))
                .map(ref -> loadRef(ref, loaded, baseDirectory))
                .map(ref -> new JsonEntry(entry.path, ref)));
  }

  private static JsonObject resolve(
      final JsonObject specification,
      final Map<String, JsonObject> loaded,
      final File baseDirectory) {
    final JsonObject json = include(specification, loaded, baseDirectory);

    return transform(
        json,
        expand(json)
            .thenApply(refResolver(loaded, baseDirectory))
            .thenApply(jsltResolver(baseDirectory))
            .thenApply(REMOVER));
  }

  private static String resourcePath(final String ref) {
    return ref.substring(RESOURCE.length());
  }

  private static JsonObject strip(final JsonObject condition) {
    return createObjectBuilder(condition).remove(LOCATION).remove(CODE).build();
  }

  private static Transformer with(final JsonObject macros) {
    return new Transformer(
        entry -> isWith(entry.value),
        entry ->
            Optional.of(
                new JsonEntry(
                    entry.path,
                    transform(
                        entry.value, expanders(withMacros(entry.value.asJsonObject(), macros))))));
  }

  private static JsonObject withMacros(final JsonObject with, final JsonObject macros) {
    return add(macros, createObjectBuilder(with.asJsonObject()).remove(EXPAND).build());
  }

  private Condition condition(final JsonObject condition) {
    final Condition c =
        ofNullable(conditionCache.get(condition))
            .orElseGet(
                () ->
                    SideEffect.<Condition>run(
                            () ->
                                conditionCache.put(
                                    condition, generateCondition(condition, features)))
                        .andThenGet(() -> conditionCache.get(condition)));

    return (json, path) -> c.apply(json, getPath(condition, path));
  }

  private Condition conditionArray(final String field, final JsonArray array) {
    final JsonObject condition = array.get(0).asJsonObject();
    final Condition conditions = conditions(null, condition);

    return (json, path) ->
        getArray(json.asJsonObject(), toJsonPointer(field))
            .map(
                values ->
                    zip(values.stream(), rangeExclusive(0, values.size()))
                        .flatMap(pair -> conditions.apply(pair.first, path + "/" + pair.second)))
            .orElseGet(Stream::empty);
  }

  private Condition conditions(final String field, final JsonObject conditions) {
    final List<Condition> c =
        getObjects(conditions, CONDITIONS).map(this::condition).collect(toList());

    return (json, path) ->
        (Stream<JsonValue>)
            to(ofNullable(field)
                    .flatMap(f -> getValue(json.asJsonObject(), toJsonPointer(f)))
                    .orElse(json))
                .apply(j -> c.stream().flatMap(condition -> condition.apply(j, path)));
  }

  private Condition generateCondition(final JsonObject condition, final Features features) {
    return ofNullable(getField(condition))
        .flatMap(
            field -> getValue(condition, toJsonPointer(field)).map(value -> pair(field, value)))
        .filter(pair -> isConditions(pair.second) || isArray(pair.second))
        .map(
            pair ->
                isConditions(pair.second)
                    ? conditions(pair.first, pair.second.asJsonObject())
                    : conditionArray(pair.first, pair.second.asJsonArray()))
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
    return load(source, (File) null);
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
    return loadRef(source, loaded, baseDirectory);
  }

  /**
   * When a validation specification includes other specifications they are resolved recursively.
   *
   * @param specification the validation specification.
   * @return the resolved validation specification.
   * @since 1.4.1
   */
  public JsonObject resolve(final JsonObject specification) {
    return resolve(specification, null);
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
    return resolve(specification, loaded, baseDirectory);
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
    return validator(loadRef(source, loaded, isResource(source) ? null : new File(source)));
  }

  /**
   * Generates a validator with the specification, which should be fully resolved.
   *
   * @param specification the validation specification.
   * @return An array with the fields <code>location</code>, which is a JSON pointer, and <code>code
   *     </code>, which is the value of the <code>$code</code> field in the specification.
   * @since 1.4.1
   */
  public Function<JsonObject, JsonArray> validator(final JsonObject specification) {
    final Condition conditions = conditions(null, specification);

    return json ->
        conditions
            .apply(json, "")
            .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
            .build();
  }

  private interface Condition extends BiFunction<JsonValue, String, Stream<JsonValue>> {}
}
