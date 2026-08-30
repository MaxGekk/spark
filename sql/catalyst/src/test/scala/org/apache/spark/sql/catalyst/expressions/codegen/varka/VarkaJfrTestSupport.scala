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

package org.apache.spark.sql.catalyst.expressions.codegen.varka

import java.nio.file.Files

import scala.jdk.CollectionConverters._

import jdk.jfr.consumer.{RecordedEvent, RecordingFile}

/**
 * The one place Varka suites run a JFR recording (task-21 review: the suites hand-rolled the
 * enable/start/stop/dump/read ritual with string event names and close() calls a failing
 * assertion could skip). Shared through the catalyst test jar like `VarkaEmitterTestSupport`;
 * Scala because the by-name body is the point of the shape.
 */
object VarkaJfrTestSupport {

  /**
   * Runs `body` inside a fresh recording with exactly `events` enabled - by class, so a
   * renamed `@Name` breaks these tests at compile time instead of leaving stale string
   * filters - and returns the body's result with everything recorded. The recording is
   * closed on every path: a recording leaked past a failing assertion would keep collecting
   * the rest of the shared test JVM's events.
   */
  def withJfrRecording[T](events: Class[_ <: jdk.jfr.Event]*)(body: => T)
      : (T, Seq[RecordedEvent]) = {
    val recording = new jdk.jfr.Recording()
    try {
      events.foreach(recording.enable(_))
      recording.start()
      val result = body
      recording.stop()
      val dump = Files.createTempFile("varka-jfr", ".jfr")
      try {
        recording.dump(dump)
        (result, RecordingFile.readAllEvents(dump).asScala.toSeq)
      } finally {
        Files.deleteIfExists(dump)
      }
    } finally {
      recording.close()
    }
  }

  /** Whether a recorded event came from the given event class, matched through its `@Name`. */
  def isEvent(event: RecordedEvent, cls: Class[_ <: jdk.jfr.Event]): Boolean =
    event.getEventType.getName == nameOf(cls)

  /** The `@Name` of an event class - read-side filters derive from the class, not a string. */
  def nameOf(cls: Class[_ <: jdk.jfr.Event]): String = {
    val name = cls.getAnnotation(classOf[jdk.jfr.Name])
    if (name != null) name.value() else cls.getName
  }
}
