package net.pincette.mongo;

import java.util.function.Function;
import javax.json.JsonValue;

/**
 * The interface for MongoDB operator implementations. The argument of such a function is the
 * operator expression for which an implementation should be generated. An implementation receives a
 * JSON object and a variable map. The names in that map will be stripped of their "$$" prefix.
 *
 * @author Werner Donn\u00e9
 * @since 1.3
 */
public interface Operator extends Function<JsonValue, Implementation> {}
