package net.pincette.mongo;

import com.schibsted.spt.data.jslt.Function;
import com.schibsted.spt.data.jslt.ResourceResolver;
import java.util.Map;
import java.util.function.BiFunction;
import javax.json.JsonObject;

/**
 * Extra features for the query and aggregation expression language.
 *
 * @author Werner Donn\u00e9
 * @since 1.5
 */
public class Features {
  public final java.util.Collection<Function> customJsltFunctions;
  public final Map<String, Operator> expressionExtensions;
  public final BiFunction<JsonObject, String, JsonObject> expressionResolver;
  public final ResourceResolver jsltResolver;
  public final Map<String, QueryOperator> matchExtensions;

  public Features() {
    this(null, null, null, null, null);
  }

  private Features(
      final java.util.Collection<Function> customJsltFunctions,
      final Map<String, Operator> expressionExtensions,
      final ResourceResolver jsltResolver,
      final Map<String, QueryOperator> matchExtensions,
      final BiFunction<JsonObject, String, JsonObject> expressionResolver) {
    this.customJsltFunctions = customJsltFunctions;
    this.expressionExtensions = expressionExtensions;
    this.jsltResolver = jsltResolver;
    this.matchExtensions = matchExtensions;
    this.expressionResolver = expressionResolver;
  }

  /**
   * Custom functions for JSLT transformations.
   *
   * @param customJsltFunctions the collection of JSLT functions.
   * @return A new features object.
   */
  public Features withCustomJsltFunctions(
      final java.util.Collection<Function> customJsltFunctions) {
    return new Features(
        customJsltFunctions,
        expressionExtensions,
        jsltResolver,
        matchExtensions,
        expressionResolver);
  }

  /**
   * Additional MongoDB Operators.
   *
   * @param expressionExtensions the mapping between operator names and their implementation.
   * @return A new features object.
   */
  public Features withExpressionExtensions(final Map<String, Operator> expressionExtensions) {
    return new Features(
        customJsltFunctions,
        expressionExtensions,
        jsltResolver,
        matchExtensions,
        expressionResolver);
  }

  /**
   * A function to resolve certain fields of MongoDB operators or query expressions. The first
   * parameter is the expression and the second a context resource in which the expression is used.
   * This could be a base directory a JAR resource or anything else. The function should return the
   * modified expression.
   *
   * @param expressionResolver the first parameter is the expression and the second a context
   *     resource in which the expression is used. This could be a base directory a JAR resource or
   *     anything else. The function should return the modified expression.
   * @return A new features object.
   * @since 2.2
   */
  public Features withExpressionResolver(
      final BiFunction<JsonObject, String, JsonObject> expressionResolver) {
    return new Features(
        customJsltFunctions,
        expressionExtensions,
        jsltResolver,
        matchExtensions,
        expressionResolver);
  }

  /**
   * The import resolver for the JSLT compiler.
   *
   * @param jsltResolver an implementation that resolves imported JSLT scripts.
   * @return A new features object.
   */
  public Features withJsltResolver(final ResourceResolver jsltResolver) {
    return new Features(
        customJsltFunctions,
        expressionExtensions,
        jsltResolver,
        matchExtensions,
        expressionResolver);
  }

  /**
   * Additional MongoDB query operators.
   *
   * @param matchExtensions the mapping between query operator names and their implementation.
   * @return A new features object.
   */
  public Features withMatchExtensions(final Map<String, QueryOperator> matchExtensions) {
    return new Features(
        customJsltFunctions,
        expressionExtensions,
        jsltResolver,
        matchExtensions,
        expressionResolver);
  }
}
