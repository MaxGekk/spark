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
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * The JUnit XML report a suite run leaves, as the Varka harness reads it: each test case passed,
 * cancelled or failed, by its name. ScalaTest's Runner writes these files with {@code -u <dir>},
 * one {@code TEST-<suite>.xml} per suite, and a suite run in its own JVM ({@code VarkaOwnJvm})
 * takes each test's result from them (VARKA-246, VARKA-312).
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
