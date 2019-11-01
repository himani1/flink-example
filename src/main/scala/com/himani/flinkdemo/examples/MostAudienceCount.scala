package com.himani.flinkdemo.examples

import com.himani.flinkdemo.datatypes.KnolxSession
import com.himani.flinkdemo.sinks.ElasticsearchUpsertSink
import com.himani.flinkdemo.sources.KnolxPortalSource
import com.himani.flinkdemo.utils.DemoStreamEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object MostAudienceCount {

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

    val popularAudience: DataStream[KnolxSession] = sessions.filter(session => session.audienceCount > 10)

    popularAudience.print()
    val totalKnolxThatAreNotMeetup: DataStream[KnolxSession] = sessions
      .filter(!_.isMeetup)
    /*val totalKnolxThatAreMeetup: DataStream[KnolxSession] = sessions
      .filter(_.isMeetup)

    totalKnolxThatAreMeetup.print()*/

    if (writeToElasticsearch) {
      print("====here!!!")
      // write to Elasticsearch
      popularAudience.addSink(new AudienceUpsert(elasticsearchHost, elasticsearchPort))
      env.execute("most aundience")

    }

    class AudienceUpsert(host: String, port: Int)
      extends ElasticsearchUpsertSink[KnolxSession](
        host,
        port,
        "elasticsearch",
        "knolx-portal",
        "knolx-sessions-audience-count") {

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
