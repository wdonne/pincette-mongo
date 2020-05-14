package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.json.JsonArray;
import javax.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestExpression {
  @Test
  @DisplayName("$jslt")
  public void jslt() {
    assertEquals(
        o(f("f", v("v")), f("test", v("test"))),
        function(
                o(
                    f(
                        "$jslt",
                        o(f("input", o(f("f", v("v")))), f("script", v("resource:/test.jslt"))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$let")
  public void let() {
    assertEquals(
        v("test11test2test3"),
        function(
                o(
                    f(
                        "$let",
                        o(
                            f(
                                "vars",
                                o(
                                    f("test1", v("test1")),
                                    f("test2", o(f("$literal", o(f("value", v("test2")))))))),
                            f(
                                "in",
                                o(
                                    f(
                                        "$let",
                                        o(
                                            f(
                                                "vars",
                                                o(f("test1", v("test11")), f("test3", v("test3")))),
                                            f(
                                                "in",
                                                o(
                                                    f(
                                                        "$concat",
                                                        a(
                                                            v("$$test1"),
                                                            v("$$test2.value"),
                                                            v("$$test3")))))))))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$literal")
  public void literal() {
    final JsonArray a = a(v(0), v(1));
    final JsonObject o = o(f("$add", a));

    assertEquals(a, function(o(f("$literal", a))).apply(o()));
    assertEquals(o, function(o(f("$literal", o))).apply(o()));
    assertEquals(v(0), function(o(f("$literal", v(0)))).apply(o()));
  }

  @Test
  @DisplayName("$mergeObjects")
  public void mergeObjects() {
    assertEquals(
        o(f("a", v(0)), f("b", v(1))),
        function(o(f("$mergeObjects", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", o(f("a", v(0)))), f("test2", o(f("b", v(1)))))));
    assertEquals(
        o(f("a", v(1)), f("b", v(2))),
        function(o(f("$mergeObjects", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", o(f("a", v(0)))), f("test2", o(f("a", v(1)), f("b", v(2)))))));
  }

  @Test
  @DisplayName("$objectToArray")
  public void objectToArray() {
    assertEquals(
        a(o(f("k", v("test1")), f("v", v(0))), o(f("k", v("test2")), f("v", v(1)))),
        function(o(f("$objectToArray", v("$test"))))
            .apply(o(f("test", o(f("test1", v(0)), f("test2", v(1)))))));
    assertEquals(a(), function(o(f("$objectToArray", v("$test")))).apply(o(f("test", o()))));
  }

  @Test
  @DisplayName("$unescape")
  public void unescape() {
    assertEquals(
        o(f("$eq", a(o(f("$gt", a(v(1), v(0))))))),
        function(o(f("$unescape", o(f("#$eq", a(o(f("#$gt", a(v(1), v(0)))))))))).apply(o()));
  }

  @Test
  @DisplayName("field value")
  public void value() {
    assertEquals(v(true), function(o(f("$eq", a(v("$test"), v(1))))).apply(o(f("test", v(1)))));
    assertEquals(v(true), function(o(f("$eq", a(v("$test2"), v(null))))).apply(o(f("test", v(1)))));
  }
}
