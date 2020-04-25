package net.pincette.mongo;

import static com.mongodb.client.model.Filters.lt;
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

public class TestLtMatch {
  @Test
  @DisplayName("$lt arrays")
  public void arrays() {
    assertFalse(predicate(lt("test", fromJson(a(v("test"), v(0))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lt objects")
  public void objects() {
    assertFalse(predicate(lt("test", fromJson(o(f("test", v(0)))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lt values")
  public void values() {
    assertTrue(predicate(lt("test", 1)).test(o(f("test", v(0)))));
    assertFalse(predicate(lt("test", 0)).test(o(f("test", v(0)))));
    assertFalse(predicate(lt("test", 0)).test(o(f("test", v(null)))));
    assertFalse(predicate(lt("test", 0)).test(o(f("test2", v(0)))));
    assertTrue(predicate(lt("test", "test")).test(o(f("test", v("rest")))));
    assertFalse(predicate(lt("test", "test")).test(o(f("test", v("test")))));
    assertTrue(predicate(lt("a.b", true)).test(o(f("a", o(f("b", v(false)))))));
    assertFalse(predicate(lt("a.b", false)).test(o(f("a", o(f("b", v(true)))))));
    assertFalse(predicate(lt("a.b", false)).test(o(f("a", o(f("b", v(false)))))));
    assertFalse(predicate(lt("a.b", true)).test(o(f("a", o(f("b", v(true)))))));
  }
}
