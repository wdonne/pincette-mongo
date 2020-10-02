package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestConditionalExpression {
  @Test
  @DisplayName("$cond")
  void cond() {
    assertEquals(
        v(0),
        function(o(f("$cond", o(f("if", v("$test")), f("then", v(0)), f("else", v(1))))))
            .apply(o(f("test", v(true)))));
    assertEquals(
        v(1),
        function(o(f("$cond", o(f("if", v("$test")), f("then", v(0)), f("else", v(1))))))
            .apply(o(f("test", v(false)))));
    assertEquals(
        v(0), function(o(f("$cond", a(v("$test"), v(0), v(1))))).apply(o(f("test", v(true)))));
    assertEquals(
        v(1), function(o(f("$cond", a(v("$test"), v(0), v(1))))).apply(o(f("test", v(false)))));
  }

  @Test
  @DisplayName("$ifNull")
  void ifNull() {
    assertEquals(v(0), function(o(f("$ifNull", a(v("$test"), v(1))))).apply(o(f("test", v(0)))));
    assertEquals(v(1), function(o(f("$ifNull", a(v("$test"), v(1))))).apply(o(f("test", v(null)))));
  }

  @Test
  @DisplayName("$switch")
  void switchTest() {
    assertEquals(
        v(0),
        function(
                o(
                    f(
                        "$switch",
                        o(
                            f(
                                "branches",
                                a(
                                    o(f("case", v("$test1")), f("then", v(0))),
                                    o(f("case", v("$test2")), f("then", v(1))))),
                            f("default", v(2))))))
            .apply(o(f("test1", v(true)), f("test2", v(false)))));
    assertEquals(
        v(1),
        function(
                o(
                    f(
                        "$switch",
                        o(
                            f(
                                "branches",
                                a(
                                    o(f("case", v("$test1")), f("then", v(0))),
                                    o(f("case", v("$test2")), f("then", v(1))))),
                            f("default", v(2))))))
            .apply(o(f("test1", v(false)), f("test2", v(true)))));
    assertEquals(
        v(2),
        function(
                o(
                    f(
                        "$switch",
                        o(
                            f(
                                "branches",
                                a(
                                    o(f("case", v("$test1")), f("then", v(0))),
                                    o(f("case", v("$test2")), f("then", v(1))))),
                            f("default", v(2))))))
            .apply(o(f("test1", v(false)), f("test2", v(false)))));
  }
}
