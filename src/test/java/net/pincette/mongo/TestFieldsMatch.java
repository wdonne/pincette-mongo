package net.pincette.mongo;

import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;
import javax.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestFieldsMatch {
  @Test
  @DisplayName("several fields")
  void values() {
    final Predicate<JsonObject> p =
        predicate(
            o(
                f("test1", o(f("$exists", v(true)))),
                f("test2", o(f("$gt", v(0)))),
                f("test3", o(f("$lt", v(2))))));

    assertTrue(p.test(o(f("test1", v(0)), f("test2", v(1)), f("test3", v(1)))));
    assertFalse(p.test(o(f("test2", v(1)), f("test3", v(1)))));
    assertFalse(p.test(o(f("test1", v(0)), f("test2", v(-1)), f("test3", v(1)))));
    assertFalse(p.test(o(f("test1", v(0)), f("test2", v(1)), f("test3", v(2)))));
  }
}
