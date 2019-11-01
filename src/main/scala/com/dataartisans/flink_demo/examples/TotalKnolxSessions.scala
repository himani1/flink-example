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

package com.dataartisans.flink_demo.examples

import com.dataartisans.flink_demo.datatypes.KnolxSession
import com.dataartisans.flink_demo.sinks.ElasticsearchUpsertSink
import com.dataartisans.flink_demo.sources.KnolxPortalSource
import com.dataartisans.flink_demo.utils.DemoStreamEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * Apache Flink DataStream API demo application.
 *
 * The program processes a stream of taxi ride events from the New York City Taxi and Limousine
 * Commission (TLC).
 * It computes for each location the total number of persons that arrived by taxi.
 *
 * See
 * http://github.com/dataartisans/flink-streaming-demo
 * for more detail.
 *
 */
object TotalKnolxSessions {

  def main(args: Array[String]) {

    // input parameters
    val data = "./data/knolxPortal.gz"
    val maxServingDelay = 60
    val servingSpeedFactor = 600f

    // Elasticsearch parameters
    val writeToElasticsearch = true // set to true to write results to Elasticsearch
    val elasticsearchHost = "localhost" // look-up hostname in Elasticsearch log output
    val elasticsearchPort = 9300


    // set up streaming execution environment
    val env: StreamExecutionEnvironment = DemoStreamEnvironment.env
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Define the data source
    val sessions: DataStream[KnolxSession] = env.addSource(new KnolxPortalSource(
      data, maxServingDelay, servingSpeedFactor))

    val totalKnolxThatAreNotMeetup: DataStream[KnolxSession] = sessions
      .filter(!_.isMeetup)

    totalKnolxThatAreNotMeetup.print()

    /*val totalKnolxThatAreMeetup: DataStream[KnolxSession] = sessions
      .filter(_.isMeetup)

    totalKnolxThatAreMeetup.print()*/

    if (writeToElasticsearch) {
      print("====here!!!")
      // write to Elasticsearch
      totalKnolxThatAreNotMeetup.addSink(new CntTimeByLocUpsert(elasticsearchHost, elasticsearchPort))
      /*.addSink((fun: KnolxSession) => {
        print("fun:: "+fun)
        new CntTimeByLocUpsert(elasticsearchHost,elasticsearchPort)
        Unit
      })
    }*/

      env.execute("Total passenger count per location")

    }

    class CntTimeByLocUpsert(host: String, port: Int)
      extends ElasticsearchUpsertSink[KnolxSession](
        host,
        port,
        "elasticsearch",
        "knolx-portal",
        "knolx-sessions") {

      override def insertJson(r: (KnolxSession)): Map[String, AnyRef] = {
        Map(
          "knolx-sessions" -> r.asInstanceOf[AnyRef]
        )
      }

      override def updateJson(r: KnolxSession): Map[String, AnyRef] = {
        Map[String, AnyRef](
          "knolx-sessions" -> r.asInstanceOf[AnyRef]
        )
      }

      override def indexKey(r: KnolxSession): String = {
        // index by location
        r.sessionDate.toString
      }
    }

  }
}
