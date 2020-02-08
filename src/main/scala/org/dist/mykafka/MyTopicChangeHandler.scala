package org.dist.mykafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.queue.common.Logging

import scala.jdk.CollectionConverters._

class MyTopicChangeHandler(myZookeeperClient: MyZookeeperClient, onTopicChange:(String, Seq[PartitionReplicas])=> Unit) extends IZkChildListener with Logging{

  var partitionCount = 0

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    info("change event received my topic change handler "+currentChilds)


    currentChilds.asScala.foreach(topicName => {
      info("inside loop of my topic change handler "+currentChilds.get(0))
      val replicas: Seq[PartitionReplicas] = myZookeeperClient.getPartitionAssignmentsFor(topicName)
      info("val replicas "+replicas)


      info("calling on topic now")
      onTopicChange(topicName, replicas)

    })

  }
}
