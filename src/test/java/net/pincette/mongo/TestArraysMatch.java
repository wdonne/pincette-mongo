package net.pincette.mongo;

import static com.mongodb.client.model.Filters.all;
import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.size;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestArraysMatch {
  @Test
  @DisplayName("$all")
  public void allTest() {
    assertTrue(
        predicate(all("test", "test1", "test2")).test(o(f("test", a(v("test1"), v("test2"))))));
    assertTrue(
        predicate(all("test", "test1", "test2"))
            .test(o(f("test", a(v("test1"), v("test2"), v("test3"))))));
    assertFalse(predicate(all("test", "test1", "test2")).test(o(f("test", a(v("test1"))))));
    assertTrue(
        predicate(all("test", fromJson(a(a(v("test1"), v("test2"))))))
            .test(o(f("test", a(a(v("test1"), v("test2")))))));
    assertTrue(
        predicate(all("test", fromJson(a(a(v("test1"), v("test2"))))))
            .test(o(f("test", a(v("test1"), v("test2"))))));
    assertFalse(
        predicate(all("test", fromJson(a(a(v("test1"), v("test2"))))))
            .test(o(f("test", a(a(v("test1"), v("test3")))))));
    assertFalse(
        predicate(all("test", fromJson(a(a(v("test1"), v("test2"))))))
            .test(o(f("test", a(v("test1"), v("test3"))))));
  }

  @Test
  @DisplayName("$elemMatch")
  public void elemMatchTest() {
    assertTrue(
        predicate(elemMatch("test", fromJson(o(f("$gt", v(0)), f("$lt", v(2))))))
            .test(o(f("test", a(v(1))))));
    assertTrue(
        predicate(elemMatch("test", fromJson(o(f("$gt", v(0)), f("$lt", v(2))))))
            .test(o(f("test", a(v(1), v(2))))));
    assertFalse(
        predicate(elemMatch("test", fromJson(o(f("$gt", v(0)), f("$lt", v(2))))))
            .test(o(f("test", a(v(2))))));
    assertTrue(
        predicate(elemMatch("test", fromJson(o(f("field1", v(0)), f("field2", o(f("$lt", v(2))))))))
            .test(o(f("test", a(o(f("field1", v(0)), f("field2", v(1))))))));
  }

  @Test
  @DisplayName("$size")
  public void sizeTest() {
    assertTrue(predicate(size("test", 2)).test(o(f("test", a(v(0), v(1))))));
    assertFalse(predicate(size("test", 1)).test(o(f("test", a(v(0), v(1))))));
  }
}
