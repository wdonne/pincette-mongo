package net.pincette.mongo;

import com.schibsted.spt.data.jslt.ResourceResolver;
import java.util.Map;

/**
 * Extra features for the query and aggregation expression language.
 *
 * @author Werner Donn\u00e9
 * @since 1.5
 */
public class Features {
  public final Map<String, Operator> expressionExtensions;
  public final ResourceResolver jsltResolver;
  public final Map<String, QueryOperator> matchExtensions;

  public Features() {
    this(null, null, null);
  }

  private Features(
      final Map<String, Operator> expressionExtensions,
      final ResourceResolver jsltResolver,
      final Map<String, QueryOperator> matchExtensions) {
    this.expressionExtensions = expressionExtensions;
    this.jsltResolver = jsltResolver;
    this.matchExtensions = matchExtensions;
  }

  public Features withExpressionExtensions(final Map<String, Operator> expressionExtensions) {
    return new Features(expressionExtensions, jsltResolver, matchExtensions);
  }

  public Features withJsltResolver(final ResourceResolver jsltResolver) {
    return new Features(expressionExtensions, jsltResolver, matchExtensions);
  }

  public Features withMatchExtensions(final Map<String, QueryOperator> matchExtensions) {
    return new Features(expressionExtensions, jsltResolver, matchExtensions);
  }
}
