package lab

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.common.security.JaasUtils

class Admin(zkConnect: String) {
  val zkConnectionTimeout = 6000
  val zkSessionTimeout = 6000
  val zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, JaasUtils.isZkSecurityEnabled())

  def createTopic(name: String) = AdminUtils.createTopic(zkUtils, name, 1, 1, new Properties())

  def topicExists(name:String): Boolean = AdminUtils.topicExists(zkUtils, name)

  def close = zkUtils.close()
}
