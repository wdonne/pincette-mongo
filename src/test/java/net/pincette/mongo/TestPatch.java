package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Patch.updateOperators;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestPatch {
  @Test
  @DisplayName("patch add")
  void add() {
    final JsonObject o = o(f("a", v(0)), f("b", a(v(0), v(1))));

    assertEquals(
        a(o(f("$set", o(f("a", v(0)))))),
        updateOperators(o, a(o(f("op", v("add")), f("path", v("/a")), f("value", v(0))))));
    assertEquals(
        a(o(f("$set", o(f("a.b", v(0)))))),
        updateOperators(o, a(o(f("op", v("add")), f("path", v("/a/b")), f("value", v(0))))));
    assertEquals(
        a(o(f("$push", o(f("b", o(f("$each", a(v(0))), f("$position", v(1)))))))),
        updateOperators(o, a(o(f("op", v("add")), f("path", v("/b/1")), f("value", v(0))))));
    assertEquals(
        a(o(f("$push", o(f("b", o(f("$each", a(v(1))), f("$position", v(0)))))))),
        updateOperators(o, a(o(f("op", v("add")), f("path", v("/b/0")), f("value", v(1))))));
    assertEquals(
        a(o(f("$push", o(f("b", o(f("$each", a(v(2))), f("$position", v(2)))))))),
        updateOperators(o, a(o(f("op", v("add")), f("path", v("/b/2")), f("value", v(2))))));
  }

  @Test
  @DisplayName("patch copy")
  void copy() {
    final JsonObject o = o(f("a", v(0)), f("b", a(v(0), v(1))));

    assertEquals(
        a(o(f("$set", o(f("c", v(0)))))),
        updateOperators(o, a(o(f("op", v("copy")), f("from", v("/a")), f("path", v("/c"))))));
    assertEquals(
        a(o(f("$set", o(f("c", v(0)))))),
        updateOperators(o, a(o(f("op", v("copy")), f("from", v("/b/0")), f("path", v("/c"))))));
    assertEquals(
        a(o(f("$set", o(f("c", v(1)))))),
        updateOperators(o, a(o(f("op", v("copy")), f("from", v("/b/1")), f("path", v("/c"))))));
    assertEquals(
        a(o(f("$set", o(f("a.c", v(1)))))),
        updateOperators(o, a(o(f("op", v("copy")), f("from", v("/b/1")), f("path", v("/a/c"))))));
    assertEquals(
        a(o(f("$push", o(f("b", o(f("$each", a(v(0))), f("$position", v(1)))))))),
        updateOperators(o, a(o(f("op", v("copy")), f("from", v("/a")), f("path", v("/b/1"))))));
  }

  @Test
  @DisplayName("patch move")
  void move() {
    final JsonObject o = o(f("a", v(0)), f("b", a(v(0), v(1), v(2))));

    assertEquals(
        a(o(f("$unset", o(f("a", v(""))))), o(f("$set", o(f("c", v(0)))))),
        updateOperators(o, a(o(f("op", v("move")), f("from", v("/a")), f("path", v("/c"))))));
    assertEquals(
        a(o(f("$set", o(f("b", a(v(1), v(2)))))), o(f("$set", o(f("c", v(0)))))),
        updateOperators(o, a(o(f("op", v("move")), f("from", v("/b/0")), f("path", v("/c"))))));
    assertEquals(
        a(o(f("$set", o(f("b", a(v(0), v(2)))))), o(f("$set", o(f("c", v(1)))))),
        updateOperators(o, a(o(f("op", v("move")), f("from", v("/b/1")), f("path", v("/c"))))));
    assertEquals(
        a(o(f("$set", o(f("b", a(v(0), v(2)))))), o(f("$set", o(f("a.c", v(1)))))),
        updateOperators(o, a(o(f("op", v("move")), f("from", v("/b/1")), f("path", v("/a/c"))))));
    assertEquals(
        a(
            o(f("$set", o(f("b", a(v(0), v(2)))))),
            o(f("$push", o(f("b", o(f("$each", a(v(1))), f("$position", v(0)))))))),
        updateOperators(o, a(o(f("op", v("move")), f("from", v("/b/1")), f("path", v("/b/0"))))));
    assertEquals(
        a(
            o(f("$set", o(f("b", a(v(0), v(2)))))),
            o(f("$push", o(f("b", o(f("$each", a(v(1))), f("$position", v(2)))))))),
        updateOperators(o, a(o(f("op", v("move")), f("from", v("/b/1")), f("path", v("/b/2"))))));
  }

  @Test
  @DisplayName("patch remove")
  void remove() {
    final JsonObject o = o(f("a", v(0)), f("b", a(v(0), v(1))));

    assertEquals(
        a(o(f("$unset", o(f("a", v("")))))),
        updateOperators(o, a(o(f("op", v("remove")), f("path", v("/a"))))));
    assertEquals(
        a(o(f("$unset", o(f("a.b", v("")))))),
        updateOperators(o, a(o(f("op", v("remove")), f("path", v("/a/b"))))));
    assertEquals(
        a(o(f("$set", o(f("b", a(v(1))))))),
        updateOperators(o, a(o(f("op", v("remove")), f("path", v("/b/0"))))));
    assertEquals(
        a(o(f("$set", o(f("b", a(v(0))))))),
        updateOperators(o, a(o(f("op", v("remove")), f("path", v("/b/1"))))));
  }

  @Test
  @DisplayName("patch replace")
  void replace() {
    final JsonObject o = o(f("a", v(0)), f("b", a(v(0), v(1))));

    assertEquals(
        a(o(f("$set", o(f("a", v(0)))))),
        updateOperators(o, a(o(f("op", v("replace")), f("path", v("/a")), f("value", v(0))))));
    assertEquals(
        a(o(f("$set", o(f("a.b", v(0)))))),
        updateOperators(o, a(o(f("op", v("replace")), f("path", v("/a/b")), f("value", v(0))))));
    assertEquals(
        a(o(f("$set", o(f("b.1", v(0)))))),
        updateOperators(o, a(o(f("op", v("replace")), f("path", v("/b/1")), f("value", v(0))))));
    assertEquals(
        a(o(f("$set", o(f("b.0", v(1)))))),
        updateOperators(o, a(o(f("op", v("replace")), f("path", v("/b/0")), f("value", v(1))))));
    assertEquals(
        a(o(f("$set", o(f("b.2", v(2)))))),
        updateOperators(o, a(o(f("op", v("replace")), f("path", v("/b/2")), f("value", v(2))))));
  }
}
