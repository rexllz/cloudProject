package com.imooc.log.utils

import com.ggstar.util.ip.IpHelper

object IpUtils {

  def getCity(ip: String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    print(getCity("119.145.14.209"))
  }

}
