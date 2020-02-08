package org.dist.mykafka

import java.io._

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config



class MyPartitionReadWrite(config: Config, topicAndPartition: TopicAndPartition) {

  val logFileSuffix = ".log"

  val logFile =
    new File(config.logDirs(0), topicAndPartition.topic + "-" + topicAndPartition.partition + logFileSuffix)

  val sequenceFile = new SequenceFile()
  val reader = new sequenceFile.Reader(logFile.getAbsolutePath)
  val writer = new sequenceFile.Writer(logFile.getAbsolutePath)

  // var offsetIndex=0


  def read(offset: Long = 0): Seq[Row] = {
    reader.
  }

  def append(key: String, value: String) = {

    writer.append(key,value)
    // offsetIndex += 1
  }

  case class Row(key: String, value: String)

}
