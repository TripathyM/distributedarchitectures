package org.dist.mykafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, LeaderAndReplicas}

class MyControllerZookeeperTest extends ZookeeperTestHarness{

  test("should select Leader for given topic partition") {

    val brokerOne = Broker(1, "broker_1", 1010)
    val brokerTwo = Broker(2, "broker_2", 1011)
    val brokerThree = Broker(3, "broker_3", 1011)

    val zookeeperClient = new MyZookeeperClient(zkClient = zkClient)
    val myCreateTopicCommand = new MyCreateTopicCommand(zookeeperClient)

    zookeeperClient.registerBroker(brokerOne)
    zookeeperClient.registerBroker(brokerTwo)
    zookeeperClient.registerBroker(brokerThree)

    val myController1 = new MyController(zookeeperClient, brokerOne.id)
    myController1.startUp()

    val topicName = "someTopicName"
    myCreateTopicCommand.createTopic(topicName,3,2)

    TestUtils.waitUntilTrue(() => {
      try {
        val list: Seq[LeaderAndReplicas] = zookeeperClient.getPartitionReplicaLeaderInfo(topicName)
        list.size > 0
      } catch {
        case e: Exception => {
          false
        }
      }
    }, "waiting for callback", 6000)

    val partitionReplicaLeaderInfo:List[LeaderAndReplicas] = zookeeperClient.getPartitionReplicaLeaderInfo(topicName);
    partitionReplicaLeaderInfo.foreach(leaderAndReplicas => {
      if(leaderAndReplicas.topicPartition.partition == 1 ){
        assert(leaderAndReplicas.partitionStateInfo.leader.id == 3)
        assert(leaderAndReplicas.partitionStateInfo.allReplicas.size == 2)
      }
      if(leaderAndReplicas.topicPartition.partition == 2 ){
        assert(leaderAndReplicas.partitionStateInfo.leader.id == 2)
        assert(leaderAndReplicas.partitionStateInfo.allReplicas.size == 2)
      }
      if(leaderAndReplicas.topicPartition.partition == 3 ){
        assert(leaderAndReplicas.partitionStateInfo.leader.id == 1)
        assert(leaderAndReplicas.partitionStateInfo.allReplicas.size == 2)
      }
    })


  }
  }
