package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestStringsExpression {
  @Test
  @DisplayName("$concat")
  public void concat() {
    assertEquals(v("abc"), function(o(f("$concat", a(v("a"), v("b"), v("c"))))).apply(o()));
    assertEquals(v(""), function(o(f("$concat", a()))).apply(o()));
  }

  @Test
  @DisplayName("$indexOfCP")
  public void indexOfCP() {
    assertEquals(v(0), function(o(f("$indexOfCP", a(v("testing"), v("te"))))).apply(o()));
    assertEquals(v(1), function(o(f("$indexOfCP", a(v("testing"), v("es"))))).apply(o()));
    assertEquals(v(-1), function(o(f("$indexOfCP", a(v("testing"), v("tt"))))).apply(o()));
    assertEquals(v(3), function(o(f("$indexOfCP", a(v("testing"), v("ti"), v(3))))).apply(o()));
    assertEquals(v(-1), function(o(f("$indexOfCP", a(v("testing"), v("ti"), v(4))))).apply(o()));
    assertEquals(
        v(3), function(o(f("$indexOfCP", a(v("testing"), v("ti"), v(3), v(6))))).apply(o()));
    assertEquals(
        v(-1), function(o(f("$indexOfCP", a(v("testing"), v("ti"), v(3), v(7))))).apply(o()));
    assertEquals(
        v(-1), function(o(f("$indexOfCP", a(v("testing"), v("ti"), v(3), v(0))))).apply(o()));
    assertEquals(
        v(-1), function(o(f("$indexOfCP", a(v("testing"), v("ti"), v(3), v(3))))).apply(o()));
    assertEquals(v(-1), function(o(f("$indexOfCP", a(v("testing"), v("ti"), v(7))))).apply(o()));
  }

  @Test
  @DisplayName("$ltrim")
  public void ltrim() {
    assertEquals(v("a "), function(o(f("$ltrim", o(f("input", v(" \t\na ")))))).apply(o()));
    assertEquals(
        v(" a "),
        function(o(f("$ltrim", o(f("input", v("zyx a ")), f("chars", v("xyz")))))).apply(o()));
  }

  @Test
  @DisplayName("$regexFind")
  public void regexFind() {
    assertEquals(
        o(f("match", v("es")), f("idx", v(1)), f("captures", a())),
        function(o(f("$regexFind", o(f("input", v("test")), f("regex", v("es")))))).apply(o()));
    assertEquals(
        o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es")))),
        function(o(f("$regexFind", o(f("input", v("test")), f("regex", v("(es)")))))).apply(o()));
    assertEquals(
        o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es")))),
        function(
                o(
                    f(
                        "$regexFind",
                        o(f("input", v("test")), f("regex", v("(ES)")), f("options", v("i"))))))
            .apply(o()));
    assertEquals(
        o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es")))),
        function(
                o(
                    f(
                        "$regexFind",
                        o(f("input", v("test")), f("regex", v("/(ES)/")), f("options", v("i"))))))
            .apply(o()));
    assertEquals(
        o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es")))),
        function(o(f("$regexFind", o(f("input", v("test")), f("regex", v("/(ES)/i"))))))
            .apply(o()));
    assertEquals(
        v(null),
        function(o(f("$regexFind", o(f("input", v("test")), f("regex", v("/(SE)/i"))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$regexFindAll")
  public void regexFindAll() {
    assertEquals(
        a(o(f("match", v("es")), f("idx", v(1)), f("captures", a()))),
        function(o(f("$regexFindAll", o(f("input", v("test")), f("regex", v("es")))))).apply(o()));
    assertEquals(
        a(o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es"))))),
        function(o(f("$regexFindAll", o(f("input", v("test")), f("regex", v("(es)"))))))
            .apply(o()));
    assertEquals(
        a(o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es"))))),
        function(
                o(
                    f(
                        "$regexFindAll",
                        o(f("input", v("test")), f("regex", v("(ES)")), f("options", v("i"))))))
            .apply(o()));
    assertEquals(
        a(o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es"))))),
        function(
                o(
                    f(
                        "$regexFindAll",
                        o(f("input", v("test")), f("regex", v("/(ES)/")), f("options", v("i"))))))
            .apply(o()));
    assertEquals(
        a(o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es"))))),
        function(o(f("$regexFindAll", o(f("input", v("test")), f("regex", v("/(ES)/i"))))))
            .apply(o()));
    assertEquals(
        a(),
        function(o(f("$regexFindAll", o(f("input", v("test")), f("regex", v("/(SE)/i"))))))
            .apply(o()));
    assertEquals(
        a(
            o(f("match", v("es")), f("idx", v(1)), f("captures", a(v("es")))),
            o(f("match", v("es")), f("idx", v(5)), f("captures", a(v("es"))))),
        function(o(f("$regexFindAll", o(f("input", v("testtest")), f("regex", v("/(ES)/i"))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$regexMatch")
  public void regexMatch() {
    assertEquals(
        v(true),
        function(o(f("$regexMatch", o(f("input", v("test")), f("regex", v("es")))))).apply(o()));
    assertEquals(
        v(true),
        function(o(f("$regexMatch", o(f("input", v("test")), f("regex", v("(es)")))))).apply(o()));
    assertEquals(
        v(true),
        function(
                o(
                    f(
                        "$regexMatch",
                        o(f("input", v("test")), f("regex", v("(ES)")), f("options", v("i"))))))
            .apply(o()));
    assertEquals(
        v(true),
        function(
                o(
                    f(
                        "$regexMatch",
                        o(f("input", v("test")), f("regex", v("/(ES)/")), f("options", v("i"))))))
            .apply(o()));
    assertEquals(
        v(true),
        function(o(f("$regexMatch", o(f("input", v("test")), f("regex", v("/(ES)/i"))))))
            .apply(o()));
    assertEquals(
        v(false),
        function(o(f("$regexMatch", o(f("input", v("test")), f("regex", v("/(SE)/i"))))))
            .apply(o()));
    assertEquals(
        v(true),
        function(o(f("$regexMatch", o(f("input", v("test")), f("regex", v("/^test$/i"))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$rtrim")
  public void rtrim() {
    assertEquals(v(" a"), function(o(f("$rtrim", o(f("input", v(" a\t\n ")))))).apply(o()));
    assertEquals(
        v(" a "),
        function(o(f("$rtrim", o(f("input", v(" a xyz")), f("chars", v("xyz")))))).apply(o()));
  }

  @Test
  @DisplayName("$split")
  public void split() {
    assertEquals(
        a(v("a"), v("b"), v("c")), function(o(f("$split", a(v("a,b,c"), v(","))))).apply(o()));
    assertEquals(a(v("a,b"), v("c")), function(o(f("$split", a(v("a,b, c"), v(", "))))).apply(o()));
    assertEquals(a(v("a b c")), function(o(f("$split", a(v("a b c"), v(","))))).apply(o()));
    assertEquals(
        a(v(""), v("ing")), function(o(f("$split", a(v("testing"), v("test"))))).apply(o()));
  }

  @Test
  @DisplayName("$strLenCP")
  public void strLenCP() {
    assertEquals(v(4), function(o(f("$strLenCP", v("test")))).apply(o()));
    assertEquals(v(4), function(o(f("$strLenCP", v("t\u00e9st")))).apply(o()));
    assertEquals(v(0), function(o(f("$strLenCP", v("")))).apply(o()));
  }

  @Test
  @DisplayName("$strcasecmp")
  public void strcasecmp() {
    assertEquals(v(-1), function(o(f("$strcasecmp", a(v("rest"), v("Test"))))).apply(o()));
    assertEquals(v(1), function(o(f("$strcasecmp", a(v("Test"), v("rest"))))).apply(o()));
    assertEquals(v(0), function(o(f("$strcasecmp", a(v("Test"), v("test"))))).apply(o()));
  }

  @Test
  @DisplayName("$substrCP")
  public void substrCP() {
    assertEquals(v("es"), function(o(f("$substrCP", a(v("test"), v(1), v(2))))).apply(o()));
    assertEquals(v("te"), function(o(f("$substrCP", a(v("test"), v(0), v(2))))).apply(o()));
    assertEquals(v("st"), function(o(f("$substrCP", a(v("test"), v(2), v(2))))).apply(o()));
    assertEquals(v(""), function(o(f("$substrCP", a(v(null), v(2), v(2))))).apply(o()));
  }

  @Test
  @DisplayName("$toLower")
  public void toLower() {
    assertEquals(v("test"), function(o(f("$toLower", v("TEST")))).apply(o()));
    assertEquals(v("test"), function(o(f("$toLower", v("TesT")))).apply(o()));
    assertEquals(v(""), function(o(f("$toLower", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$toUpper")
  public void toUpper() {
    assertEquals(v("TEST"), function(o(f("$toUpper", v("TEST")))).apply(o()));
    assertEquals(v("TEST"), function(o(f("$toUpper", v("TesT")))).apply(o()));
    assertEquals(v(""), function(o(f("$toUpper", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$trim")
  public void trim() {
    assertEquals(v("a"), function(o(f("$trim", o(f("input", v(" \n\ta\t\n ")))))).apply(o()));
    assertEquals(
        v(" a "),
        function(o(f("$trim", o(f("input", v("zyx a xyz")), f("chars", v("xyz")))))).apply(o()));
  }
}
