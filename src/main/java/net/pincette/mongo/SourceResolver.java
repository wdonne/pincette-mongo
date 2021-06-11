package net.pincette.mongo;

import static java.util.Optional.ofNullable;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.util.Util.canonicalPath;
import static net.pincette.util.Util.getParent;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import javax.json.JsonObject;
import javax.json.JsonReader;
import net.pincette.mongo.Validator.Resolved;
import net.pincette.util.Cases;

class SourceResolver {
  private static final String RESOURCE = "resource:";
  private final Map<String, JsonObject> loaded = new HashMap<>();

  private static boolean isDirectory(final String context) {
    return isKind(context, File::isDirectory);
  }

  private static boolean isFile(final String context) {
    return isKind(context, File::isFile);
  }

  private static boolean isKind(final String context, final Predicate<File> test) {
    return ofNullable(context).filter(c -> test.test(new File(c))).isPresent();
  }

  private static boolean isResource(final String ref) {
    return ofNullable(ref).filter(r -> r.startsWith(RESOURCE)).isPresent();
  }

  private static String resolveResource(final String resource, final String baseResource) {
    final String path = baseResource.substring(RESOURCE.length());

    return RESOURCE
        + canonicalPath((path.endsWith("/") ? path : (getParent(path, "/") + "/")) + resource, "/");
  }

  private static Optional<String> resolveSource(final String source, final String context) {
    return Cases.<String, String>withValue(source)
        .or(SourceResolver::isResource, s -> s)
        .or(s -> isResource(context) && !s.startsWith("/"), s -> resolveResource(s, context))
        .or(s -> isDirectory(context), s -> new File(context, s).getAbsolutePath())
        .or(SourceResolver::isFile, s -> new File(s).getAbsolutePath())
        .get();
  }

  private static String resourcePath(final String ref) {
    return ref.substring(RESOURCE.length());
  }

  private JsonObject load(final InputStream in) {
    return tryToGetWithRethrow(() -> createReader(in), JsonReader::readObject).orElse(null);
  }

  private JsonObject load(final File file) {
    return load(tryToGetRethrow(() -> new FileInputStream(file)).orElse(null));
  }

  private JsonObject load(final String resource) {
    return load(SourceResolver.class.getResourceAsStream(resource));
  }

  Optional<Resolved> resolve(final String source, final String context) {
    return resolveSource(source, context)
        .map(
            s ->
                new Resolved(
                    loaded.computeIfAbsent(
                        s, k -> isResource(k) ? load(resourcePath(k)) : load(new File(k))),
                    s));
  }
}
