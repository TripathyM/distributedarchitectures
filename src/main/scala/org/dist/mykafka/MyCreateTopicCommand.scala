package org.dist.mykafka

case class PartitionReplicas(partitionId: Int, brokerIds: List[Int])

class MyCreateTopicCommand(zookeeperClient: MyZookeeperClient) {

  def createTopic(topicName: String, partitionCount: Int, replicationFactor: Int) = {

    val brokerIds = zookeeperClient.getAllBrokerIds()
    val partitionReplicas: Set[PartitionReplicas] = assignPartitionsWithReplicasToBrokers(brokerIds.toList, partitionCount, replicationFactor)


    zookeeperClient.setPartitionReplicasForTopic(topicName, partitionReplicas)
  }

  def assignPartitionsToBrokers(brokerList: List[Int], partitionCount: Int, replicationFactor: Int): Set[PartitionReplicas] = {
    val partitionReplicaOne = PartitionReplicas(1, List(1))
    val partitionReplicaTwo = PartitionReplicas(2, List(2))
    Set(partitionReplicaOne, partitionReplicaTwo)
  }

  def assignPartitionsWithReplicasToBrokers(brokerList: List[Int], partitionCount: Int, replicationFactor: Int): Set[PartitionReplicas] = {
    val partitionReplicaOne = PartitionReplicas(1, List(3, 1))
    val partitionReplicaTwo = PartitionReplicas(2, List(2,3))
    val partitionReplicaThree = PartitionReplicas(3, List(1,2))
    Set(partitionReplicaOne, partitionReplicaTwo, partitionReplicaThree)
  }


}
