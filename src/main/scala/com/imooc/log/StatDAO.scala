package com.imooc.log

import java.sql.{Connection, PreparedStatement}

import com.imooc.log.utils.MysqlUtils

import scala.collection.mutable.ListBuffer

/*
* Our code begin.
* */
object StatDAO {

  // Deprecated
  def insertTotalTable(list: ListBuffer[TotalTable]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into total_label(source_type, source_id, traffic, ip, city, day, minute, learn_name, learn_url, video_url, video_name) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.source_type)
        pstmt.setLong(2, item.source_id)
        pstmt.setLong(3, item.traffic)
        pstmt.setString(4, item.ip)
        pstmt.setString(5, item.city)
        pstmt.setString(6, item.day)
        pstmt.setString(7, item.minute)
        pstmt.setString(8, item.learn_name)
        pstmt.setString(9, item.learn_url)
        pstmt.setString(10, item.video_url)
        pstmt.setString(11, item.video_name)


        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }

  def insertMinuteCity(list: ListBuffer[MinuteCityElement]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into minute_city(minute, city, visit_times) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)


      for(item <- list) {

        pstmt.setLong(1, item.minute)
        pstmt.setString(2, item.city)
        pstmt.setLong(3, item.times)


        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }


  def insertLabelMinuteTimes(list: ListBuffer[LabelMinuteElement]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into label_minute(label, minute, visit_times) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.label)
        pstmt.setLong(2, item.minute)
        pstmt.setLong(3, item.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }


  def insertLabelCityTimes(list: ListBuffer[LabelCityElement]): Unit ={
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {

      connection = MysqlUtils.getConnection()

      connection.setAutoCommit(false)

      val sql = "insert into label_city(label, city, visit_times) values (?, ?, ?)"
      pstmt = connection.prepareStatement(sql)

      for(item <- list) {
        pstmt.setString(1, item.label)
        pstmt.setString(2, item.city)
        pstmt.setLong(3, item.times)

        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MysqlUtils.release(connection, pstmt)
    }
  }
}
/*
* Our code end.
* */