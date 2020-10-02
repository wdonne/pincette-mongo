package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestArraysExpression {
  @Test
  @DisplayName("$arrayElemAt")
  void arrayElemAt() {
    assertEquals(
        v(2),
        function(o(f("$arrayElemAt", a(v("$test"), v(2)))))
            .apply(o(f("test", a(v(0), v(1), v(2))))));
    assertEquals(
        v(null),
        function(o(f("$arrayElemAt", a(v("$test"), v(3)))))
            .apply(o(f("test", a(v(0), v(1), v(2))))));
    assertEquals(
        v(1),
        function(o(f("$arrayElemAt", a(v("$test"), v(-2)))))
            .apply(o(f("test", a(v(0), v(1), v(2))))));
    assertNotEquals(
        v(2),
        function(o(f("$arrayElemAt", a(v("$test"), v(0)))))
            .apply(o(f("test", a(v(0), v(1), v(2))))));
  }

  @Test
  @DisplayName("$arrayToObject")
  void arrayToObject() {
    assertEquals(
        o(f("test1", v(0)), f("test2", v(1))),
        function(o(f("$arrayToObject", v("$test"))))
            .apply(o(f("test", a(a(v("test1"), v(0)), a(v("test2"), v(1)))))));
    assertEquals(
        o(f("test1", v(0)), f("test2", v(1))),
        function(o(f("$arrayToObject", v("$test"))))
            .apply(
                o(
                    f(
                        "test",
                        a(
                            o(f("k", v("test1")), f("v", v(0))),
                            o(f("k", v("test2")), f("v", v(1))))))));
    assertNotEquals(
        o(f("test1", v(1)), f("test2", v(1))),
        function(o(f("$arrayToObject", v("$test"))))
            .apply(o(f("test", a(a(v("test1"), v(0)), a(v("test2"), v(1)))))));
  }

  @Test
  @DisplayName("$concatArrays")
  void concatArrays() {
    assertEquals(
        a(v(0), v(1), v(2), v(3)),
        function(o(f("$concatArrays", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1))), f("test2", a(v(2), v(3))))));
    assertEquals(
        v(null),
        function(o(f("$concatArrays", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1))), f("test3", a(v(2), v(3))))));
    assertNotEquals(
        a(v(0), v(1), v(2), v(4)),
        function(o(f("$concatArrays", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1))), f("test2", a(v(2), v(3))))));
  }

  @Test
  @DisplayName("$elemMatch")
  void elemMatch() {
    assertEquals(
        v(1), function(o(f("$elemMatch", a(a(v(0), v(1)), o(f("$gt", v(0))))))).apply(o()));
  }

  @Test
  @DisplayName("$filter")
  void filter() {
    assertEquals(
        a(v(1), v(2)),
        function(
                o(
                    f(
                        "$filter",
                        o(
                            f("input", a(v(1), v(2), v(0))),
                            f("as", v("array")),
                            f("cond", o(f("$gt", a(v("$$array"), v(0)))))))))
            .apply(o()));
    assertEquals(
        a(v(1), v(2)),
        function(o(f("$filter", o(f("input", a(v(1), v(2), v(0))), f("cond", v("$$this"))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$in")
  void in() {
    assertEquals(
        v(true),
        function(o(f("$in", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(0)), f("test2", a(v(0), v(1))))));
    assertEquals(
        v(false),
        function(o(f("$in", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(2)), f("test2", a(v(0), v(1))))));
  }

  @Test
  @DisplayName("$indexOfArray")
  void indexOfArray() {
    assertEquals(
        v(1),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(2))), f("test2", v(2)))));
    assertEquals(
        v(-1),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(2))), f("test2", v(5)))));
    assertEquals(
        v(null),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(null)), f("test2", v(2)))));
    assertEquals(
        v(2),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", a(v(0), v(1), v(2), v(4))), f("test2", v(2)), f("test3", v(1)))));
    assertEquals(
        v(2),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2"), v("$test3"), v("$test4")))))
            .apply(
                o(
                    f("test1", a(v(0), v(1), v(2), v(4))),
                    f("test2", v(2)),
                    f("test3", v(1)),
                    f("test4", v(3)))));
    assertEquals(
        v(1),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2"), v("$test3"), v("$test4")))))
            .apply(
                o(
                    f("test1", a(v(0), v(1), v(2), v(4))),
                    f("test2", v(1)),
                    f("test3", v(1)),
                    f("test4", v(1)))));
    assertEquals(
        v(-1),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2"), v("$test3"), v("$test4")))))
            .apply(
                o(
                    f("test1", a(v(0), v(1), v(2), v(4))),
                    f("test2", v(1)),
                    f("test3", v(2)),
                    f("test4", v(1)))));
    assertEquals(
        v(-1),
        function(o(f("$indexOfArray", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", a(v(0), v(1), v(2), v(4))), f("test2", v(2)), f("test3", v(4)))));
  }

  @Test
  @DisplayName("$isArray")
  void isArray() {
    assertEquals(v(true), function(o(f("$isArray", v("$test")))).apply(o(f("test", a(v(0))))));
    assertEquals(v(false), function(o(f("$isArray", v("$test")))).apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$map")
  void map() {
    assertEquals(
        a(v(2), v(4), v(6)),
        function(
                o(
                    f(
                        "$map",
                        o(
                            f("input", a(v(1), v(2), v(3))),
                            f("as", v("array")),
                            f("in", o(f("$multiply", a(v("$$array"), v(2)))))))))
            .apply(o()));
    assertEquals(
        a(v(2), v(3), v(4)),
        function(
                o(
                    f(
                        "$map",
                        o(
                            f("input", a(v(1), v(2), v(3))),
                            f("in", o(f("$add", a(v("$$this"), v(1)))))))))
            .apply(o()));
    assertEquals(
        a(v(1), v(2), v(3)),
        function(
                o(
                    f(
                        "$map",
                        o(
                            f(
                                "input",
                                a(o(f("value", v(1))), o(f("value", v(2))), o(f("value", v(3))))),
                            f("in", v("$$this.value"))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$objectToArray")
  void objectToArray() {
    assertEquals(
        a(
            o(f("k", v("test1")), f("v", v(0))),
            o(f("k", v("test2")), f("v", o(f("test3", v(1)), f("test4", v(2)))))),
        function(o(f("$objectToArray", v("$test"))))
            .apply(
                o(
                    f(
                        "test",
                        o(f("test1", v(0)), f("test2", o(f("test3", v(1)), f("test4", v(2)))))))));
  }

  @Test
  @DisplayName("$range")
  void range() {
    assertEquals(
        a(v(0), v(1), v(2), v(3), v(4)),
        function(o(f("$range", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(0)), f("test2", v(5)))));
    assertEquals(
        a(v(0), v(2), v(4), v(6), v(8)),
        function(o(f("$range", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", v(0)), f("test2", v(10)), f("test3", v(2)))));
    assertEquals(
        a(v(10), v(8), v(6), v(4), v(2)),
        function(o(f("$range", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", v(10)), f("test2", v(0)), f("test3", v(-2)))));
    assertEquals(
        a(),
        function(o(f("$range", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", v(0)), f("test2", v(10)), f("test3", v(-2)))));
  }

  @Test
  @DisplayName("$reduce")
  void reduce() {
    assertEquals(
        o(f("sum", v(15)), f("product", v(48))),
        function(
                o(
                    f(
                        "$reduce",
                        o(
                            f("input", a(v(1), v(2), v(3), v(4))),
                            f("initialValue", o(f("sum", v(5)), f("product", v(2)))),
                            f(
                                "in",
                                o(
                                    f("sum", o(f("$add", a(v("$$value.sum"), v("$$this"))))),
                                    f(
                                        "product",
                                        o(
                                            f(
                                                "$multiply",
                                                a(v("$$value.product"), v("$$this")))))))))))
            .apply(o()));
    assertEquals(
        o(f("sum", v(5)), f("product", v(2))),
        function(
                o(
                    f(
                        "$reduce",
                        o(
                            f("input", a()),
                            f("initialValue", o(f("sum", v(5)), f("product", v(2)))),
                            f(
                                "in",
                                o(
                                    f("sum", o(f("$add", a(v("$$value.sum"), v("$$this"))))),
                                    f(
                                        "product",
                                        o(
                                            f(
                                                "$multiply",
                                                a(v("$$value.product"), v("$$this")))))))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$reverseArray")
  void reverseArray() {
    assertEquals(
        a(v(2), v(1), v(0)),
        function(o(f("$reverseArray", v("$test")))).apply(o(f("test", a(v(0), v(1), v(2))))));
    assertEquals(v(null), function(o(f("$reverseArray", v("$test")))).apply(o(f("test", v(null)))));
  }

  @Test
  @DisplayName("$size")
  void size() {
    assertEquals(v(2), function(o(f("$size", v("$test")))).apply(o(f("test", a(v(0), v(1))))));
    assertEquals(v(0), function(o(f("$size", v("$test")))).apply(o(f("test", a()))));
    assertEquals(v(null), function(o(f("$size", v("$test")))).apply(o(f("test2", a()))));
  }

  @Test
  @DisplayName("$slice")
  void slice() {
    assertEquals(
        a(v(0)),
        function(o(f("$slice", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", v(1)))));
    assertEquals(
        a(v(0), v(1)),
        function(o(f("$slice", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", v(2)))));
    assertEquals(
        a(v(1), v(2)),
        function(o(f("$slice", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", v(-2)))));
    assertEquals(
        a(v(1), v(2)),
        function(o(f("$slice", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", v(1)), f("test3", v(2)))));
    assertEquals(
        a(v(1), v(2)),
        function(o(f("$slice", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", v(1)), f("test3", v(3)))));
    assertEquals(
        a(),
        function(o(f("$slice", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", v(3)), f("test3", v(2)))));
    assertEquals(
        a(v(0), v(1), v(2)),
        function(o(f("$slice", a(v("$test1"), v("$test2"), v("$test3")))))
            .apply(o(f("test1", a(v(0), v(1), v(2))), f("test2", v(-10)), f("test3", v(3)))));
  }

  @Test
  @DisplayName("$sort")
  void sort() {
    assertEquals(
        a(v(0), v(1), v(2)),
        function(o(f("$sort", o(f("input", a(v(1), v(2), v(0))))))).apply(o()));
    assertEquals(
        a(v(2), v(1), v(0)),
        function(o(f("$sort", o(f("input", a(v(1), v(2), v(0))), f("direction", v("desc"))))))
            .apply(o()));
    assertEquals(
        a(
            o(f("f1", v(0)), f("f2", v(0))),
            o(f("f1", v(0)), f("f2", v(1))),
            o(f("f1", v(1)), f("f2", v(0)))),
        function(o(f("$sort", o(f("input", v("$test")), f("paths", a(v("f1"), v("f2")))))))
            .apply(
                o(
                    f(
                        "test",
                        a(
                            o(f("f1", v(0)), f("f2", v(1))),
                            o(f("f1", v(0)), f("f2", v(0))),
                            o(f("f1", v(1)), f("f2", v(0))))))));
  }

  @Test
  @DisplayName("$zip")
  void zip() {
    assertEquals(
        a(a(v(1), v("a")), a(v(2), v("b")), a(v(3), v("c"))),
        function(o(f("$zip", o(f("inputs", a(a(v(1), v(2), v(3)), a(v("a"), v("b"), v("c"))))))))
            .apply(o()));
    assertEquals(
        a(a(v(1), v("a")), a(v(2), v("b"))),
        function(o(f("$zip", o(f("inputs", a(a(v(1), v(2)), a(v("a"), v("b"), v("c"))))))))
            .apply(o()));
    assertEquals(
        a(a(v(1), v(2), v(3))),
        function(o(f("$zip", o(f("inputs", a(a(v(1)), a(v(2)), a(v(3)))))))).apply(o()));
    assertEquals(
        a(a(v(1), v("a")), a(v(2), v("b"))),
        function(o(f("$zip", o(f("inputs", a(a(v(1), v(2), v(3)), a(v("a"), v("b"))))))))
            .apply(o()));
    assertEquals(
        a(a(v(1), v("a")), a(v(2), v("b")), a(v(3), v(null))),
        function(
                o(
                    f(
                        "$zip",
                        o(
                            f("inputs", a(a(v(1), v(2), v(3)), a(v("a"), v("b")))),
                            f("useLongestLength", v(true))))))
            .apply(o()));
    assertEquals(
        a(a(v(1), v("a")), a(v(2), v("b")), a(v(null), v("c"))),
        function(
                o(
                    f(
                        "$zip",
                        o(
                            f("inputs", a(a(v(1), v(2)), a(v("a"), v("b"), v("c")))),
                            f("useLongestLength", v(true))))))
            .apply(o()));
    assertEquals(
        a(a(v(1), v("a")), a(v(2), v("x")), a(v(3), v("y"))),
        function(
                o(
                    f(
                        "$zip",
                        o(
                            f("inputs", a(a(v(1), v(2), v(3)), a(v("a")))),
                            f("useLongestLength", v(true)),
                            f("defaults", a(v("x"), v("y"), v("z")))))))
            .apply(o()));
  }
}
