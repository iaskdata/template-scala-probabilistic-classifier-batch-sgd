package org.template.classification

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

import grizzled.slf4j.Logger

case class DataSourceParams(
  appName: String,
  evalK: Option[Int]  // define the k-fold parameter.
) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, ActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    logger.info(dsp.appName)
    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user")
//      eventNames = Some(List("bad_loan", "loan_amnt","longest_credit_length","revol_util",
//        "emp_length","home_ownership","annual_inc","purpose","addr_state","dti","delinq_2yrs"
//        ,"total_acc","verification_status","term")))(sc)

//      eventNames = Some(List("bad_loan", "loan_amnt","longest_credit_length","revol_util",
//      "emp_length","home_ownership","annual_inc","dti","delinq_2yrs"
//      ,"total_acc","verification_status","term"))

    )(sc)
    logger.info(eventsRDD.first())

    val labeledPoints: RDD[LabeledPoint] = eventsRDD.map{event =>
      val home_ownership: Double = event.properties.get[String]("home_ownership") match {
        case "OWN" => 5.0
        case "RENT" => 1.0
        case "MORTGAGE" => 0.0
        case _ => 0.0
      }

      val verification_status: Double = event.properties.get[String]("verification_status") match {
        case "verified" => 1.0
        case "not verified" => 0.0
        case _ => 0.0
      }


      val emp_length: Double = event.properties.get[String]("emp_length") match {
        case "" => 0.0
        case _ => event.properties.get[String]("emp_length").toDouble
      }

      val term:Double = event.properties.get[String]("term").replace("months","").toDouble;

      LabeledPoint(event.properties.get[String]("bad_loan").toDouble,
        Vectors.dense(Array(
          event.properties.get[String]("loan_amnt").toDouble,
          event.properties.get[String]("longest_credit_length").toDouble,
          event.properties.get[String]("revol_util").toDouble,
          event.properties.get[String]("emp_length").toDouble,
          home_ownership,
          event.properties.get[String]("annual_inc").toDouble,
//          event.properties.get[Double]("purpose"),
//          event.properties.get[Double]("addr_state"),
          event.properties.get[String]("dti").toDouble,
          event.properties.get[String]("delinq_2yrs").toDouble,
          event.properties.get[String]("total_acc").toDouble,
          verification_status,
          term
        ))
      )

    }.cache()


    new TrainingData(labeledPoints)
  }

  override
  def readEval(sc: SparkContext)
  : Seq[(TrainingData, EmptyEvaluationInfo, RDD[(Query, ActualResult)])] = {
    require(!dsp.evalK.isEmpty, "DataSourceParams.evalK must not be None")

    // The following code reads the data from data store. It is equivalent to
    // the readTraining method. We copy-and-paste the exact code here for
    // illustration purpose, a recommended approach is to factor out this logic
    // into a helper function and have both readTraining and readEval call the
    // helper.
    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user")
      //      eventNames = Some(List("bad_loan", "loan_amnt","longest_credit_length","revol_util",
      //        "emp_length","home_ownership","annual_inc","purpose","addr_state","dti","delinq_2yrs"
      //        ,"total_acc","verification_status","term")))(sc)

      //      eventNames = Some(List("bad_loan", "loan_amnt","longest_credit_length","revol_util",
      //      "emp_length","home_ownership","annual_inc","dti","delinq_2yrs"
      //      ,"total_acc","verification_status","term"))

    )(sc)
    logger.info(eventsRDD.first())

    val labeledPoints: RDD[LabeledPoint] = eventsRDD.map{event =>
      val home_ownership: Double = event.event match {
        case "OWN" => 5.0
        case "RENT" => 1.0
        case "MORTGAGE" => 0.0
        case _ => 0.0
      }

      val verification_status: Double = event.event match {
        case "verified" => 1.0
        case "not verified" => 0.0
        case _ => 0.0
      }

      val term:Double = event.event.replace("months","").toDouble;



      LabeledPoint(event.properties.get[Double]("bad_loan"),
        Vectors.dense(Array(
          event.properties.get[Double]("loan_amnt"),
          event.properties.get[Double]("longest_credit_length"),
          event.properties.get[Double]("revol_util"),
          event.properties.get[Double]("emp_length"),
          home_ownership,
          event.properties.get[Double]("annual_inc"),
          //          event.properties.get[Double]("purpose"),
          //          event.properties.get[Double]("addr_state"),
          event.properties.get[Double]("dti"),
          event.properties.get[Double]("delinq_2yrs"),
          event.properties.get[Double]("total_acc"),
          verification_status,
          term
        ))
      )

    }.cache()
    // End of reading from data store

    // K-fold splitting
    val evalK = dsp.evalK.get
    val indexedPoints: RDD[(LabeledPoint, Long)] = labeledPoints.zipWithIndex

    (0 until evalK).map { idx =>
      val trainingPoints = indexedPoints.filter(_._2 % evalK != idx).map(_._1)
      val testingPoints = indexedPoints.filter(_._2 % evalK == idx).map(_._1)

      (
        new TrainingData(trainingPoints),
        new EmptyEvaluationInfo(),
        testingPoints.map {
          p => (new Query(p.features.toArray), new ActualResult(p.label))
        }
      )
    }
  }
}

class TrainingData(
  val labeledPoints: RDD[LabeledPoint]
) extends Serializable
