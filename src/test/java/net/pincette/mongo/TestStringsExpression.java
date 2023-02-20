package net.pincette.mongo;

import static javax.json.JsonValue.NULL;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestStringsExpression {
  @Test
  @DisplayName("$base64Encode $base64Decode")
  void base64() {
    final String s = "dfgxuxfgjkshdbvbsénfzfsf^$^%&^fvg";

    assertEquals(v(s), function(o(f("$base64Decode", o(f("$base64Encode", v(s)))))).apply(o()));
  }

  @Test
  @DisplayName("$concat")
  void concat() {
    assertEquals(v("abc"), function(o(f("$concat", a(v("a"), v("b"), v("c"))))).apply(o()));
    assertEquals(v(""), function(o(f("$concat", a()))).apply(o()));
  }

  @Test
  @DisplayName("$indexOfCP")
  void indexOfCP() {
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
  @DisplayName("$jsonToString $stringToJson")
  void json() {
    final JsonObject o = o(f("test1", v("s")), f("test2", v(0)));

    assertEquals(o, function(o(f("$stringToJson", o(f("$jsonToString", o))))).apply(o()));
  }

  @Test
  @DisplayName("$ltrim")
  void ltrim() {
    assertEquals(v("a "), function(o(f("$ltrim", o(f("input", v(" \t\na ")))))).apply(o()));
    assertEquals(
        v(" a "),
        function(o(f("$ltrim", o(f("input", v("zyx a ")), f("chars", v("xyz")))))).apply(o()));
  }

  @Test
  @DisplayName("$regexFind")
  void regexFind() {
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
  void regexFindAll() {
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
  void regexMatch() {
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
  @DisplayName("$replaceAll")
  void replaceAll() {
    assertEquals(
        v("bacba"),
        function(
                o(
                    f(
                        "$replaceAll",
                        o(f("input", v("$test")), f("find", v("ab")), f("replacement", v("ba"))))))
            .apply(o(f("test", v("abcab")))));
    assertEquals(
        NULL,
        function(
                o(
                    f(
                        "$replaceAll",
                        o(f("input", v("$tst")), f("find", v("ab")), f("replacement", v("ba"))))))
            .apply(o(f("test", v("abcab")))));
    assertEquals(
        NULL,
        function(
                o(
                    f(
                        "$replaceAll",
                        o(f("input", v("$test")), f("find", NULL), f("replacement", v("ba"))))))
            .apply(o(f("test", v("abcab")))));
    assertEquals(
        NULL,
        function(
                o(
                    f(
                        "$replaceAll",
                        o(f("input", v("$test")), f("find", v("ab")), f("replacement", NULL)))))
            .apply(o(f("test", v("abcab")))));
  }

  @Test
  @DisplayName("$replaceOne")
  void replaceOne() {
    assertEquals(
        v("bac"),
        function(
                o(
                    f(
                        "$replaceOne",
                        o(f("input", v("$test")), f("find", v("ab")), f("replacement", v("ba"))))))
            .apply(o(f("test", v("abc")))));
    assertEquals(
        NULL,
        function(
                o(
                    f(
                        "$replaceOne",
                        o(f("input", v("$tst")), f("find", v("ab")), f("replacement", v("ba"))))))
            .apply(o(f("test", v("abc")))));
    assertEquals(
        NULL,
        function(
                o(
                    f(
                        "$replaceOne",
                        o(f("input", v("$test")), f("find", NULL), f("replacement", v("ba"))))))
            .apply(o(f("test", v("abc")))));
    assertEquals(
        NULL,
        function(
                o(
                    f(
                        "$replaceOne",
                        o(f("input", v("$test")), f("find", v("ab")), f("replacement", NULL)))))
            .apply(o(f("test", v("abc")))));
  }

  @Test
  @DisplayName("$rtrim")
  void rtrim() {
    assertEquals(v(" a"), function(o(f("$rtrim", o(f("input", v(" a\t\n ")))))).apply(o()));
    assertEquals(
        v(" a "),
        function(o(f("$rtrim", o(f("input", v(" a xyz")), f("chars", v("xyz")))))).apply(o()));
  }

  @Test
  @DisplayName("$split")
  void split() {
    assertEquals(
        a(v("a"), v("b"), v("c")), function(o(f("$split", a(v("a,b,c"), v(","))))).apply(o()));
    assertEquals(a(v("a,b"), v("c")), function(o(f("$split", a(v("a,b, c"), v(", "))))).apply(o()));
    assertEquals(a(v("a b c")), function(o(f("$split", a(v("a b c"), v(","))))).apply(o()));
    assertEquals(
        a(v(""), v("ing")), function(o(f("$split", a(v("testing"), v("test"))))).apply(o()));
  }

  @Test
  @DisplayName("$strLenCP")
  void strLenCP() {
    assertEquals(v(4), function(o(f("$strLenCP", v("test")))).apply(o()));
    assertEquals(v(4), function(o(f("$strLenCP", v("t\u00e9st")))).apply(o()));
    assertEquals(v(0), function(o(f("$strLenCP", v("")))).apply(o()));
  }

  @Test
  @DisplayName("$strcasecmp")
  void strcasecmp() {
    assertEquals(v(-1), function(o(f("$strcasecmp", a(v("rest"), v("Test"))))).apply(o()));
    assertEquals(v(1), function(o(f("$strcasecmp", a(v("Test"), v("rest"))))).apply(o()));
    assertEquals(v(0), function(o(f("$strcasecmp", a(v("Test"), v("test"))))).apply(o()));
  }

  @Test
  @DisplayName("$substrCP")
  void substrCP() {
    assertEquals(v("es"), function(o(f("$substrCP", a(v("test"), v(1), v(2))))).apply(o()));
    assertEquals(v("te"), function(o(f("$substrCP", a(v("test"), v(0), v(2))))).apply(o()));
    assertEquals(v("st"), function(o(f("$substrCP", a(v("test"), v(2), v(2))))).apply(o()));
    assertEquals(v(""), function(o(f("$substrCP", a(v(null), v(2), v(2))))).apply(o()));
  }

  @Test
  @DisplayName("$toLower")
  void toLower() {
    assertEquals(v("test"), function(o(f("$toLower", v("TEST")))).apply(o()));
    assertEquals(v("test"), function(o(f("$toLower", v("TesT")))).apply(o()));
    assertEquals(v(""), function(o(f("$toLower", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$toUpper")
  void toUpper() {
    assertEquals(v("TEST"), function(o(f("$toUpper", v("TEST")))).apply(o()));
    assertEquals(v("TEST"), function(o(f("$toUpper", v("TesT")))).apply(o()));
    assertEquals(v(""), function(o(f("$toUpper", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$trim")
  void trim() {
    assertEquals(v("a"), function(o(f("$trim", o(f("input", v(" \n\ta\t\n ")))))).apply(o()));
    assertEquals(
        v(" a "),
        function(o(f("$trim", o(f("input", v("zyx a xyz")), f("chars", v("xyz")))))).apply(o()));
  }

  @Test
  @DisplayName("$uriEncode $uriDecode")
  void uri() {
    final String s = "dfgxuxfgjkshdbvbs\u00e9nfzfsf^$^%&^fvg";

    assertEquals(v(s), function(o(f("$uriDecode", o(f("$uriEncode", v(s)))))).apply(o()));
  }
}
