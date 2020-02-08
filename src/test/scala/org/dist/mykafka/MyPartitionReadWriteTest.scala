package org.dist.mykafka

import org.dist.queue.TestUtils
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.util.Networks
import org.scalatest.FunSuite

class MyPartitionReadWriteTest extends FunSuite {

  test("should write and read message from partition"){



    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))
    val myPartitionReadWrite = new MyPartitionReadWrite(config1,TopicAndPartition("topic1", 0))

    myPartitionReadWrite.append("key1", "message1")
    myPartitionReadWrite.append("key2", "message2")
    myPartitionReadWrite.append("key3", "message3")

    val messages: Seq[myPartitionReadWrite.Row] = myPartitionReadWrite.read()
    assert(messages.size == 2)
    assert(messages(0).key == "key1")
    assert(messages(1).key == "key2")
    assert(messages(0).value == "message1")
    assert(messages(1).value == "message2")
  }

}
