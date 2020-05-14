package net.pincette.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.expr;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.mod;
import static com.mongodb.client.model.Filters.regex;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestEvaluationMatch {
  @Test
  @DisplayName("$expr")
  public void exprTest() {
    assertTrue(
        predicate(expr(and(eq("test", 0), eq("test2", 1))))
            .test(o(f("test", v(0)), f("test2", v(1)))));
    assertTrue(
        predicate(expr(fromJson(o(f("$add", a(v("$test1"), v("$test2")))))))
            .test(o(f("test1", v(0)), f("test2", v(1)))));
    assertFalse(
        predicate(expr(fromJson(o(f("$add", a(v("$test1"), v("$test2")))))))
            .test(o(f("test1", v(0)), f("test2", v(0)))));
    assertTrue(
        predicate(expr(fromJson(o(f("$gt", a(o(f("$add", a(v("$test1"), v("$test2")))), v(0)))))))
            .test(o(f("test1", v(0)), f("test2", v(1)))));
    assertFalse(predicate(expr("$test")).test(o(f("test", v(0)))));
    assertFalse(predicate(expr("$test")).test(o(f("test", v(null)))));
    assertFalse(predicate(expr("$test")).test(o(f("test2", v(1)))));
  }

  @Test
  @DisplayName("$mod")
  public void modTest() {
    assertTrue(predicate(mod("test", 2, 0)).test(o(f("test", v(4)))));
    assertFalse(predicate(mod("test", 2, 0)).test(o(f("test", v(3)))));
  }

  @Test
  @DisplayName("$regex")
  public void regexTest() {
    assertTrue(predicate(regex("test", "es")).test(o(f("test", v("test")))));
    assertTrue(predicate(o(f("test", o(f("$regex", v("es")))))).test(o(f("test", v("test")))));
    assertTrue(predicate(regex("test", "ES", "i")).test(o(f("test", v("test")))));
    assertTrue(predicate(regex("test", "/es/")).test(o(f("test", v("test")))));
    assertTrue(predicate(regex("test", "/ES/i")).test(o(f("test", v("test")))));
    assertTrue(predicate(regex("test", "^te")).test(o(f("test", v("test")))));
    assertTrue(predicate(regex("test", "st$")).test(o(f("test", v("test")))));
    assertTrue(predicate(regex("test", "t.*T", "is")).test(o(f("test", v("te\nst")))));
    assertFalse(predicate(regex("test", ".*se.*")).test(o(f("test", v("test")))));
    assertTrue(predicate(in("test", "/t.*T/i", "/es/")).test(o(f("test", v("test")))));
    assertTrue(predicate(in("test", "test", "/se/")).test(o(f("test", v("test")))));
    assertFalse(predicate(in("test", "/tt/", "/se/")).test(o(f("test", v("test")))));
    assertTrue(predicate(o(f("test", o(f("$not", v("/se/")))))).test(o(f("test", v("test")))));
    assertFalse(predicate(o(f("test", o(f("$not", v("/es/")))))).test(o(f("test", v("test")))));
    assertTrue(
        predicate(
            regex(
                "test", "^([a-zA-Z0-9_\\-\\.]+)@((\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.)|("
                    + "([a-zA-Z0-9\\-]+\\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\\]?)$"))
            .test(o(f("test", v("Admin@re3.be")))));
  }
}
