package net.pincette.mongo;

import java.util.function.BiFunction;
import java.util.function.Predicate;
import javax.json.JsonValue;

/**
 * The interface for MongoDB query operator implementations. The argument of such a function is the
 * query operator expression for which an implementation should be generated. An implementation
 * receives the field value of a JSON object.
 *
 * @author Werner Donn\u00e9
 * @since 2.0
 */
public interface QueryOperator extends BiFunction<JsonValue, Features, Predicate<JsonValue>> {}
