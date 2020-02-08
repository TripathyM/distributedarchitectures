package org.dist.mykafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.mykafka.PartitionReplicas

class MyTopicChangeHandlerTest extends ZookeeperTestHarness {

  class TestContext {
    var replicas:Seq[PartitionReplicas] = List()
    def leaderAndIsr(topicName:String, replicas:Seq[PartitionReplicas]) = {
      this.replicas = replicas
    }
  }

  test("should receive a callback for topic creation"){
    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)

    val brokerOne = Broker(1, "broker1", 1010)
    val brokerTwo = Broker(2, "broker2", 1012)
    zookeeperClient.registerBroker(brokerOne)
    zookeeperClient.registerBroker(brokerTwo)

    val myCreateTopicCommand = new MyCreateTopicCommand(zookeeperClient)

    val myTopicChangeHandler = new MyTopicChangeHandler(zookeeperClient,new TestContext().leaderAndIsr)
    zookeeperClient.subscribeTopicChangeListener(myTopicChangeHandler)

    val topicName = "someTopicName"
    val partitionCount = 2
    myCreateTopicCommand.createTopic(topicName, partitionCount,1)

    TestUtils.waitUntilTrue(() => {
      myTopicChangeHandler.partitionCount == partitionCount
    }, "waiting for callback", 2000)

    assert(myTopicChangeHandler.partitionCount == 2)

  }

}
