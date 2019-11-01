/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flink_demo.datatypes

import java.util.Locale

import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

class KnolxSession(
                    var sessionId: Long,
                    var sessionType: String,
                    var sessionDate: DateTime,
                    var sessionName: String,
                    var audienceCount: Long,
                    var isMeetup: Boolean) {

  def this() {
    this(0, "", new DateTime(0), "", 0, false)
  }

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(sessionId).append(",")
    sb.append(sessionType).append(",")
    sb.append(sessionDate.toString(KnolxSession.TimeFormatter)).append(",")
    sb.append(sessionName).append(",")
    sb.append(audienceCount).append(",")
    sb.append(isMeetup)
    sb.toString()
  }

}

object KnolxSession {

  @transient
  private final val TimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC

  def fromString(line: String): KnolxSession = {

    val tokens: Array[String] = line.split(",")
    if (tokens.length != 6) {
      throw new RuntimeException("Invalid record: " + line)
    }
    /*
    session_id: Long
    session_type: string
    session-date: DateTime
    session-name: string
    audience-count: long
    isMeetup: boolean
     */
    try {
      val sessionId = tokens(0).toLong
      val sessionType = tokens(1).toString //DateTime.parse(tokens(1), TimeFormatter)
      val sessionDate = DateTime.parse(tokens(2), TimeFormatter)
      val sessionName = tokens(3).toString
      val audienceCount = tokens(4).toLong
      val isMeetup = tokens(5).toBoolean

      new KnolxSession(sessionId, sessionType, sessionDate, sessionName, audienceCount, isMeetup)
    }
    catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException("Invalid record: " + line, nfe)
    }
  }
}

//case class GeoPoint(lon: Double, lat: Double)
