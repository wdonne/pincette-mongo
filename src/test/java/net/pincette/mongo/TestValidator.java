package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.emptyArray;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Function;
import javax.json.JsonArray;
import javax.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestValidator {
  private final Validator validator = new Validator();

  @Test
  @DisplayName("test1")
  void test1() {
    final Function<JsonObject, JsonArray> check =
        validator.validator("resource:/validators/tests/test1/validator.json");

    assertEquals(
        emptyArray(),
        check.apply(
            o(
                f("field1", v(true)),
                f(
                    "field2",
                    o(
                        f("field1", v("test")),
                        f("field2", v(false)),
                        f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                        f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be")))))))));

    assertEquals(
        a(o(f("location", v("/field1")), f("code", v("BOOL")))),
        check.apply(
            o(
                f("field1", v("test")),
                f(
                    "field2",
                    o(
                        f("field1", v("test")),
                        f("field2", v(false)),
                        f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                        f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be")))))))));

    assertEquals(
        a(o(f("location", v("/field2/sub2/field2")), f("code", v("EMAIL")))),
        check.apply(
            o(
                f("field1", v(true)),
                f(
                    "field2",
                    o(
                        f("field1", v("test")),
                        f("field2", v(false)),
                        f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                        f("sub2", o(f("field1", v(1)), f("field2", v("test")))))))));

    assertEquals(
        a(
            o(f("location", v("/field1")), f("code", v("REQUIRED"))),
            o(f("location", v("/field1")), f("code", v("BOOL")))),
        check.apply(
            o(
                f(
                    "field2",
                    o(
                        f("field1", v("test")),
                        f("field2", v(false)),
                        f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                        f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be")))))))));
  }

  @Test
  @DisplayName("test2")
  void test2() {
    final Function<JsonObject, JsonArray> check =
        validator.validator("resource:/validators/tests/test2/validator.json");

    assertEquals(
        emptyArray(),
        check.apply(
            o(
                f("field1", v(34597978797L)),
                f(
                    "field2",
                    a(
                        o(
                            f("field1", v("test")),
                            f("field2", v(false)),
                            f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                            f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be"))))),
                        o(
                            f("field1", v("test")),
                            f("field2", v(false)),
                            f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                            f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be"))))))))));

    assertEquals(
        a(o(f("location", v("/field1")), f("code", v("LONG")))),
        check.apply(
            o(
                f("field1", v(true)),
                f(
                    "field2",
                    a(
                        o(
                            f("field1", v("test")),
                            f("field2", v(false)),
                            f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                            f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be"))))),
                        o(
                            f("field1", v("test")),
                            f("field2", v(false)),
                            f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                            f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be"))))))))));

    assertEquals(
        a(o(f("location", v("/field2/0/sub1/field2")), f("code", v("EMAIL")))),
        check.apply(
            o(
                f("field1", v(34597978797L)),
                f(
                    "field2",
                    a(
                        o(
                            f("field1", v("test")),
                            f("field2", v(false)),
                            f("sub1", o(f("field1", v(0)), f("field2", v("test")))),
                            f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be"))))),
                        o(
                            f("field1", v("test")),
                            f("field2", v(false)),
                            f("sub1", o(f("field1", v(0)), f("field2", v("a@re.be")))),
                            f("sub2", o(f("field1", v(1)), f("field2", v("b@re.be"))))))))));
  }
}
