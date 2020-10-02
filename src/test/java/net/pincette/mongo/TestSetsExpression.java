package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestSetsExpression {
  @Test
  @DisplayName("$allElementsTrue")
  void allElementsTrue() {
    assertEquals(
        v(true),
        function(o(f("$allElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(true)), f("test2", v(true)))));
    assertEquals(
        v(true),
        function(o(f("$allElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(1)), f("test2", v("test")))));
    assertEquals(
        v(true),
        function(o(f("$allElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(1))), f("test2", o(f("test", v("test")))))));
    assertEquals(
        v(false),
        function(o(f("$allElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(false)), f("test2", v(true)))));
    assertEquals(
        v(false),
        function(o(f("$allElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(0)), f("test2", v(true)))));
    assertEquals(
        v(false),
        function(o(f("$allElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(null)), f("test2", v(true)))));
  }

  @Test
  @DisplayName("$anyElementsTrue")
  void anyElementsTrue() {
    assertEquals(
        v(true),
        function(o(f("$anyElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(false)), f("test2", v(true)))));
    assertEquals(
        v(true),
        function(o(f("$anyElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(0)), f("test2", v("test")))));
    assertEquals(
        v(true),
        function(o(f("$anyElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0))), f("test2", o(f("test", v("test")))))));
    assertEquals(
        v(false),
        function(o(f("$anyElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(false)), f("test2", v(false)))));
    assertEquals(
        v(false),
        function(o(f("$anyElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(0)), f("test2", v(false)))));
    assertEquals(
        v(false),
        function(o(f("$anyElementsTrue", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(null)), f("test2", v(false)))));
  }

  @Test
  @DisplayName("$setDifference")
  void setDifference() {
    assertEquals(
        a(v(0)),
        function(o(f("$setDifference", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1))), f("test2", a(v(1), v(2))))));
    assertEquals(
        a(),
        function(o(f("$setDifference", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1))), f("test2", a(v(0), v(1))))));
  }

  @Test
  @DisplayName("$setEquals")
  void setEquals() {
    assertEquals(
        v(true),
        function(o(f("$setEquals", a(v("$test1"), v("$test2"), v("$test3"), v("$test4")))))
            .apply(
                o(
                    f("test1", a(v("a"), v("b"), v("a"))),
                    f("test2", a(v("b"), v("a"))),
                    f("test3", a(v("a"), v("b"))),
                    f("test4", a(v("b"), v("a"), v("b"))))));
    assertEquals(
        v(true),
        function(o(f("$setEquals", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", a()), f("test2", a()), f("test3", a()))));
    assertEquals(v(true), function(o(f("$setEquals", a()))).apply(o()));
    assertEquals(
        v(false),
        function(o(f("$setEquals", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(
                o(
                    f("test1", a(v("a"), v("b"), v("a"))),
                    f("test2", a(v("c"), v("a"))),
                    f("test3", a(v("b"), v("a"), v("b"))))));
  }

  @Test
  @DisplayName("$setIntersection")
  void setIntersection() {
    assertEquals(
        a(v("a")),
        function(o(f("$setIntersection", a(v("$test1"), v("$test2"), v("$test3"), v("$test4")))))
            .apply(
                o(
                    f("test1", a(v("a"), v("b"), v("a"))),
                    f("test2", a(v("c"), v("a"))),
                    f("test3", a(v("a"), v("b"))),
                    f("test4", a(v("c"), v("a"), v("b"))))));
    assertEquals(
        a(),
        function(o(f("$setIntersection", a(v("$test1"), v("$test2"), v("$test3"), v("$test4")))))
            .apply(
                o(
                    f("test1", a(v("a"), v("b"), v("a"))),
                    f("test2", a(v("c"), v("d"))),
                    f("test3", a(v("a"), v("b"))),
                    f("test4", a(v("c"), v("a"), v("b"))))));
    assertEquals(a(), function(o(f("$setIntersection", a()))).apply(o()));
    assertEquals(a(), function(o(f("$setIntersection", a(a(), a())))).apply(o()));
  }

  @Test
  @DisplayName("$setIsSubset")
  void setIsSubset() {
    assertEquals(
        v(true),
        function(o(f("$setIsSubset", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1))), f("test2", a(v(0), v(1), v(2))))));
    assertEquals(
        v(true),
        function(o(f("$setIsSubset", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1))), f("test2", a(v(0), v(1))))));
    assertEquals(
        v(true),
        function(o(f("$setIsSubset", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a()), f("test2", a(v(0), v(1))))));
    assertEquals(
        v(false),
        function(o(f("$setIsSubset", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", a(v(0), v(1))))));
    assertEquals(
        v(false),
        function(o(f("$setIsSubset", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", a()))));
  }

  @Test
  @DisplayName("$setUnion")
  void setUnion() {
    assertEquals(
        a(v("a"), v("b"), v("c")),
        function(o(f("$setUnion", a(v("$test1"), v("$test2"), v("$test3"), v("$test4")))))
            .apply(
                o(
                    f("test1", a(v("a"), v("b"), v("a"))),
                    f("test2", a(v("c"), v("a"))),
                    f("test3", a(v("a"), v("b"))),
                    f("test4", a(v("c"), v("a"), v("b"))))));
    assertEquals(a(), function(o(f("$setUnion", a()))).apply(o()));
    assertEquals(a(), function(o(f("$setUnion", a(a(), a())))).apply(o()));
  }
}
