package org.dist.mykafka

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, LeaderAndReplicas, PartitionInfo}

class MyController(zookeeperClient: MyZookeeperClient, brokerId : Int) {
  var liveBrokers: Set[Broker] = Set()
  var currentLeader = -1

  def startUp() = {
    elect()
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    zookeeperClient.subscribeTopicChangeListener(new MyTopicChangeHandler(zookeeperClient, onTopicChange))
   // zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeListener(this, zookeeperClient))
  }

  private def selectLeaderAndFollowerBrokersForPartitions(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(p => {
      val leaderBrokerId = p.brokerIds.head //This is where leader for particular partition is selected
      val leaderBroker = getBroker(leaderBrokerId)
      val replicaBrokers = p.brokerIds.map(id ⇒ getBroker(id))
      LeaderAndReplicas(TopicAndPartition(topicName, p.partitionId), PartitionInfo(leaderBroker, replicaBrokers))
    })
    leaderAndReplicas
  }
  private def getBroker(brokerId: Int) = {
    liveBrokers.find(b ⇒ b.id == brokerId).get
  }


  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]) = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = selectLeaderAndFollowerBrokersForPartitions(topicName, partitionReplicas)

    print(leaderAndReplicas.toList)

    zookeeperClient.setPartitionLeaderForTopic(topicName, leaderAndReplicas.toList)
    //This is persisted in zookeeper for failover.. we are just keeping it in memory for now.
   // sendLeaderAndReplicaRequestToAllLeadersAndFollowersForGivenPartition(leaderAndReplicas, partitionReplicas)
   // sendUpdateMetadataRequestToAllLiveBrokers(leaderAndReplicas)

  }


}
