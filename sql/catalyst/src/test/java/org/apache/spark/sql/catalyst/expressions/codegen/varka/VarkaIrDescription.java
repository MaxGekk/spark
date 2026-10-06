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

package org.apache.spark.sql.catalyst.expressions.codegen.varka;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * A graph of {@link VarkaVectorIR} as a description every layout of the IR's storage can be built
 * from (VARKA-291): one line for each distinct subtree, its kind, its scalar components and the
 * ids of its children, each child before its parent. The compiler's graphs are built in Scala and
 * the spike's arms are plain Java outside the build, so the description is what crosses between
 * them, and every arm builds exactly the same graphs from it.
 *
 * <p>It is read off the records by reflection over their components, so a kind added to the IR is
 * described without anyone editing this class: a component of a node type is a child, and every
 * other component is a scalar - an {@code int}, {@code long} or {@code boolean}, an enum constant's
 * name, or the one list of {@code int}s, {@code InRanges.bounds}. A child is written as the id of
 * its line, and children and scalars each in component order, so a record is rebuilt by walking its
 * components once and taking the next child or the next scalar by the component's type. Subtrees
 * that are equal are one line: the description is the graph hash-consed, which is also how the
 * flat layouts hold it.
 *
 * <p>The text form, one graph at a time:
 * <pre>
 * graph NAME inputs 2 literals 3 roots 7,9
 * 0 ColumnRef 0 INT |
 * 1 LiteralSlot 0 INT |
 * 2 IntArith ADD WRAP | 0 1
 * </pre>
 * Test code, and plain Java.
 */
final class VarkaIrDescription {

  private VarkaIrDescription() {}

  /** One distinct subtree: its kind's simple name, its scalars as text, and its children's ids. */
  record Node(String kind, List<String> scalars, List<Integer> children) {
    Node {
      scalars = List.copyOf(scalars);
      children = List.copyOf(children);
    }
  }

  /** A graph: its nodes in dependency order, and the ids of the roots, one for each output. */
  record Graph(String name, int numInputs, int numLiterals, List<Node> nodes, List<Integer> roots) {
    Graph {
      nodes = List.copyOf(nodes);
      roots = List.copyOf(roots);
      for (int i = 0; i < nodes.size(); i++) {
        for (int child : nodes.get(i).children()) {
          if (child < 0 || child >= i) {
            throw new IllegalArgumentException(
                "node " + i + " names child " + child + ", which does not come before it");
          }
        }
      }
      for (int root : roots) {
        if (root < 0 || root >= nodes.size()) {
          throw new IllegalArgumentException("root " + root + " is not a node");
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Describing a graph.
  // ---------------------------------------------------------------------------------------------

  /** The description of {@code roots}, with equal subtrees as one node. */
  static Graph describe(String name, List<VarkaVectorIR> roots, int numInputs, int numLiterals) {
    var nodes = new ArrayList<Node>();
    var ids = new HashMap<VarkaVectorIR, Integer>();
    var rootIds = new ArrayList<Integer>();
    for (VarkaVectorIR root : roots) {
      rootIds.add(describe(root, nodes, ids));
    }
    return new Graph(name, numInputs, numLiterals, nodes, rootIds);
  }

  private static int describe(
      VarkaVectorIR node, List<Node> nodes, Map<VarkaVectorIR, Integer> ids) {
    Integer known = ids.get(node);
    if (known != null) {
      return known;
    }
    var scalars = new ArrayList<String>();
    var children = new ArrayList<Integer>();
    for (RecordComponent component : node.getClass().getRecordComponents()) {
      Object value = read(component, node);
      if (value instanceof VarkaVectorIR child) {
        children.add(describe(child, nodes, ids));
      } else {
        scalars.add(scalarText(value));
      }
    }
    nodes.add(new Node(node.getClass().getSimpleName(), scalars, children));
    int id = nodes.size() - 1;
    ids.put(node, id);
    return id;
  }

  private static Object read(RecordComponent component, Object node) {
    try {
      return component.getAccessor().invoke(node);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("cannot read " + component, e);
    }
  }

  private static String scalarText(Object value) {
    return switch (value) {
      case Enum<?> constant -> constant.name();
      case List<?> list -> {
        var text = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
          text.append(i == 0 ? "" : ",").append(list.get(i));
        }
        yield text.append(']').toString();
      }
      default -> String.valueOf(value);
    };
  }

  // ---------------------------------------------------------------------------------------------
  // Rebuilding the records.
  // ---------------------------------------------------------------------------------------------

  /** The roots of {@code graph}, rebuilt as records through each kind's canonical constructor. */
  static List<VarkaVectorIR> rebuild(Graph graph) {
    var built = new ArrayList<VarkaVectorIR>(graph.nodes().size());
    for (Node node : graph.nodes()) {
      built.add(rebuild(node, built));
    }
    var roots = new ArrayList<VarkaVectorIR>();
    for (int root : graph.roots()) {
      roots.add(built.get(root));
    }
    return roots;
  }

  private static VarkaVectorIR rebuild(Node node, List<VarkaVectorIR> built) {
    Class<?> kind = kindOf(node.kind());
    RecordComponent[] components = kind.getRecordComponents();
    var types = new Class<?>[components.length];
    var arguments = new Object[components.length];
    int nextChild = 0;
    int nextScalar = 0;
    for (int i = 0; i < components.length; i++) {
      types[i] = components[i].getType();
      if (VarkaVectorIR.class.isAssignableFrom(types[i])) {
        arguments[i] = built.get(node.children().get(nextChild++));
      } else {
        arguments[i] = scalar(types[i], node.scalars().get(nextScalar++));
      }
    }
    if (nextChild != node.children().size() || nextScalar != node.scalars().size()) {
      throw new IllegalArgumentException(node + " does not fit " + kind.getSimpleName());
    }
    try {
      return (VarkaVectorIR) kind.getDeclaredConstructor(types).newInstance(arguments);
    } catch (InvocationTargetException e) {
      throw new IllegalArgumentException(
          kind.getSimpleName() + " refuses " + node + ": " + e.getCause().getMessage(), e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("cannot build " + kind, e);
    }
  }

  /** The record class of a kind, by its simple name: one of the IR's nested records. */
  static Class<?> kindOf(String simpleName) {
    try {
      return Class.forName(VarkaVectorIR.class.getName() + "$" + simpleName);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("no IR kind named " + simpleName, e);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static Object scalar(Class<?> type, String text) {
    if (type == int.class) {
      return Integer.parseInt(text);
    } else if (type == long.class) {
      return Long.parseLong(text);
    } else if (type == boolean.class) {
      return Boolean.parseBoolean(text);
    } else if (type.isEnum()) {
      return Enum.valueOf((Class<Enum>) type, text);
    } else if (type == List.class) {
      var values = new ArrayList<Integer>();
      String inner = text.substring(1, text.length() - 1);
      if (!inner.isEmpty()) {
        for (String value : inner.split(",")) {
          values.add(Integer.parseInt(value));
        }
      }
      return List.copyOf(values);
    }
    throw new IllegalArgumentException("no scalar of type " + type.getName() + " in the IR");
  }

  /** Every record kind of the sealed IR, by simple name, in name order. */
  static TreeSet<String> allKinds() {
    var kinds = new TreeSet<String>();
    collect(VarkaVectorIR.class, kinds);
    return kinds;
  }

  private static void collect(Class<?> type, TreeSet<String> kinds) {
    Class<?>[] permitted = type.getPermittedSubclasses();
    if (permitted == null) {
      kinds.add(type.getSimpleName());
    } else {
      for (Class<?> subclass : permitted) {
        collect(subclass, kinds);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The text form.
  // ---------------------------------------------------------------------------------------------

  /** {@code graph} as text, a header line and a line for each node. */
  static String toText(Graph graph) {
    var text = new StringBuilder("graph ").append(graph.name()).append(" inputs ")
        .append(graph.numInputs()).append(" literals ").append(graph.numLiterals())
        .append(" roots ");
    for (int i = 0; i < graph.roots().size(); i++) {
      text.append(i == 0 ? "" : ",").append(graph.roots().get(i));
    }
    text.append('\n');
    for (int id = 0; id < graph.nodes().size(); id++) {
      Node node = graph.nodes().get(id);
      text.append(id).append(' ').append(node.kind());
      for (String scalar : node.scalars()) {
        text.append(' ').append(scalar);
      }
      text.append(" |");
      for (int child : node.children()) {
        text.append(' ').append(child);
      }
      text.append('\n');
    }
    return text.toString();
  }

  /** The graphs of {@code text}, as {@link #toText} writes them one after another. */
  static List<Graph> parse(String text) {
    var graphs = new ArrayList<Graph>();
    String name = null;
    int numInputs = 0;
    int numLiterals = 0;
    var roots = new ArrayList<Integer>();
    var nodes = new ArrayList<Node>();
    for (String line : text.split("\n")) {
      if (line.isBlank()) {
        continue;
      }
      if (line.startsWith("graph ")) {
        if (name != null) {
          graphs.add(new Graph(name, numInputs, numLiterals, nodes, roots));
        }
        String[] header = line.split(" ");
        name = header[1];
        numInputs = Integer.parseInt(header[3]);
        numLiterals = Integer.parseInt(header[5]);
        roots = new ArrayList<>();
        nodes = new ArrayList<>();
        if (header.length > 7 && !header[7].isEmpty()) {
          for (String root : header[7].split(",")) {
            roots.add(Integer.parseInt(root));
          }
        }
      } else {
        int bar = line.indexOf('|');
        String[] head = line.substring(0, bar).trim().split(" ");
        if (Integer.parseInt(head[0]) != nodes.size()) {
          throw new IllegalArgumentException("node ids are not consecutive at: " + line);
        }
        var scalars = new ArrayList<String>();
        for (int i = 2; i < head.length; i++) {
          scalars.add(head[i]);
        }
        var children = new ArrayList<Integer>();
        String after = line.substring(bar + 1).trim();
        if (!after.isEmpty()) {
          for (String child : after.split(" ")) {
            children.add(Integer.parseInt(child));
          }
        }
        nodes.add(new Node(head[1], scalars, children));
      }
    }
    if (name != null) {
      graphs.add(new Graph(name, numInputs, numLiterals, nodes, roots));
    }
    return graphs;
  }
}
