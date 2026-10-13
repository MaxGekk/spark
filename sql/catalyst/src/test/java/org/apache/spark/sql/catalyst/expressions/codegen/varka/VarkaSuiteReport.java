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

import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * The JUnit XML report a suite run leaves, as the Varka harness reads it: each test case passed,
 * cancelled or failed, by its name. ScalaTest's Runner writes these files with {@code -u <dir>},
 * one {@code TEST-<suite>.xml} per suite, and a suite run in its own JVM ({@code VarkaOwnJvm})
 * takes each test's result from them (VARKA-246, VARKA-312).
 *
 * <p>A JUnit suite under {@code @VarkaSuite} writes the same files through {@link #write}, so
 * the scripts that read them - the own-JVM parent, the shard weights
 * ({@code dev/varka_suite_shards.py}, which reads each {@code testsuite}'s {@code time}), the
 * matrix script's check of the sanitizer suite's {@code <skipped} - read both kinds alike.
 */
public final class VarkaSuiteReport {

  private VarkaSuiteReport() {}

  /** One test's result. */
  public sealed interface Result permits Passed, Skipped, Failed {}

  /** The test passed. */
  public record Passed() implements Result {}

  /** The test was cancelled, for {@code reason}. */
  public record Skipped(String reason) implements Result {}

  /** The test failed, with {@code message}. */
  public record Failed(String message) implements Result {}

  /** One test as a report holds it: its name, its result and how long it ran. */
  public record Case(String name, Result result, double seconds) {}

  /**
   * Writes {@code TEST-<suite>.xml} into {@code dir}: one {@code testsuite} named {@code suite}
   * (the fully qualified class name, as ScalaTest names it) with its counts and {@code seconds},
   * and one {@code testcase} per case, named by the test's name. {@code aborted}, when present,
   * is why the suite as a whole failed - a class-level hook that threw - and counts as one error.
   */
  public static void write(File dir, String suite, double seconds, List<Case> cases,
      Optional<String> aborted) throws Exception {
    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
    Element root = doc.createElement("testsuite");
    doc.appendChild(root);
    int failures = 0;
    int skipped = 0;
    for (Case c : cases) {
      Element node = doc.createElement("testcase");
      node.setAttribute("name", xmlSafe(c.name()));
      node.setAttribute("classname", suite);
      node.setAttribute("time", seconds(c.seconds()));
      switch (c.result()) {
        case Passed p -> { }
        case Skipped s -> {
          skipped++;
          Element child = doc.createElement("skipped");
          child.setAttribute("message", xmlSafe(s.reason()));
          node.appendChild(child);
        }
        case Failed f -> {
          failures++;
          Element child = doc.createElement("failure");
          child.setAttribute("message", xmlSafe(f.message()));
          node.appendChild(child);
        }
      }
      root.appendChild(node);
    }
    if (aborted.isPresent()) {
      Element error = doc.createElement("error");
      error.setAttribute("message", xmlSafe(aborted.get()));
      root.appendChild(error);
    }
    root.setAttribute("name", suite);
    root.setAttribute("tests", Integer.toString(cases.size()));
    root.setAttribute("failures", Integer.toString(failures));
    root.setAttribute("errors", aborted.isPresent() ? "1" : "0");
    root.setAttribute("skipped", Integer.toString(skipped));
    root.setAttribute("time", seconds(seconds));
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
    dir.mkdirs();
    transformer.transform(new DOMSource(doc),
        new StreamResult(new File(dir, "TEST-" + suite + ".xml")));
  }

  private static String seconds(double s) {
    return String.format(Locale.ROOT, "%.3f", s);
  }

  /** {@code text} without the characters XML 1.0 cannot hold, which a message may carry. */
  private static String xmlSafe(String text) {
    var out = new StringBuilder(text.length());
    text.codePoints().forEach(c -> {
      boolean allowed = c == 0x9 || c == 0xA || c == 0xD || (c >= 0x20 && c <= 0xD7FF)
          || (c >= 0xE000 && c <= 0xFFFD) || c >= 0x10000;
      out.appendCodePoint(allowed ? c : '?');
    });
    return out.toString();
  }

  /**
   * Every test case of the reports in {@code dir}, in file and document order: failed where it
   * has a {@code failure} or {@code error} element (with that element's message, prefixed as a
   * failure in another JVM), cancelled where it has a {@code skipped} one, passed otherwise.
   * Empty when the directory holds no report.
   */
  public static Map<String, Result> read(File dir) throws Exception {
    var results = new LinkedHashMap<String, Result>();
    File[] files = dir.listFiles((d, name) -> name.endsWith(".xml"));
    if (files == null) {
      return results;
    }
    Arrays.sort(files);
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    for (File file : files) {
      NodeList cases = builder.parse(file).getElementsByTagName("testcase");
      for (int i = 0; i < cases.getLength(); i++) {
        var node = (Element) cases.item(i);
        String name = node.getAttribute("name");
        Result result;
        if (node.getElementsByTagName("failure").getLength() > 0) {
          result = new Failed("in its own JVM: " + messageOf(node, "failure"));
        } else if (node.getElementsByTagName("error").getLength() > 0) {
          result = new Failed("in its own JVM: " + messageOf(node, "error"));
        } else if (node.getElementsByTagName("skipped").getLength() > 0) {
          result = new Skipped("cancelled in its own JVM");
        } else {
          result = new Passed();
        }
        results.put(name, result);
      }
    }
    return results;
  }

  private static String messageOf(Element testCase, String tag) {
    return ((Element) testCase.getElementsByTagName(tag).item(0)).getAttribute("message");
  }
}
