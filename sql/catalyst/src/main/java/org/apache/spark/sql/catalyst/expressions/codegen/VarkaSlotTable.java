/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.codegen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * One of a compile's two tables: the kernel's inputs, keyed by child ordinal (or a derived
 * input's key, see {@link VarkaDerivedInput#key}), and its int literals, keyed by value. Each key
 * gets the next dense slot the first time it is interned, so the compiled shape depends only on
 * the order keys first occur, never on their values. A declining entry rolls the table back with
 * {@link #truncate} to the size it had before the entry, which drops exactly what the entry
 * interned, since slots are handed out in order.
 *
 * <p>Internal to Varka: public only because the tests in other packages build one to call a
 * family directly.
 */
public final class VarkaSlotTable {

  private final Map<Integer, Integer> slots = new HashMap<>();
  private final List<Integer> keys = new ArrayList<>();

  /** {@code key}'s slot, appending it as the next slot if it is new. */
  public int intern(int key) {
    Integer slot = slots.get(key);
    if (slot != null) {
      return slot;
    }
    int next = keys.size();
    slots.put(key, next);
    keys.add(key);
    return next;
  }

  /** {@code key}'s slot, or empty if it has none. */
  public OptionalInt slot(int key) {
    Integer slot = slots.get(key);
    return slot == null ? OptionalInt.empty() : OptionalInt.of(slot);
  }

  /** How many slots are taken: the mark a later {@link #truncate} rolls back to. */
  public int size() {
    return keys.size();
  }

  /** The key in {@code slot}. */
  public int keyAt(int slot) {
    return keys.get(slot);
  }

  /** The keys in slot order. */
  public List<Integer> keys() {
    return List.copyOf(keys);
  }

  /** Drops the slots from {@code mark} on: what was interned after the table had that size. */
  public void truncate(int mark) {
    while (keys.size() > mark) {
      slots.remove(keys.remove(keys.size() - 1));
    }
  }
}
