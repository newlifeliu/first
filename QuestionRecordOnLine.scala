package k12.question

import k12.constant.Constants
import k12.utils.{CommonUtils, HBaseUtils, JDBCWriteUtils, QuestionUtils}
import org.apache.hadoop.hbase.TableName
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Durations, StreamingContext}

import java.io.{File, FileInputStream}
import java.util.Properties


object QuestionRecordOnLine {
  //准备一个logger，用于运行程序时打印日志
  private val logger: Logger = Logger.getLogger(QuestionRecordOnLine.getClass)

  def main(args: Array[String]): Unit = {
    //1.配置连接参数
    val params=new Properties()
    params.load(new FileInputStream(new File(args(0))))
      //Spark 相关参数
    val SPARK_MASTER = params.getProperty(Constants.SPARK_MASTER)
    val SPARK_APP_NAME_QUESTION_RECORD = params.getProperty(Constants.SPARK_APP_NAME_QUESTION_RECORD)
    val SPARK_CHECKPOINT_DIR_QUESTION_RECORD = params.getProperty(Constants.SPARK_CHECKPOINT_DIR_QUESTION_RECORD)
    val SPARK_DURATION_MILLISECONDS = params.getProperty(Constants.SPARK_DURATION_MILLISECONDS)
    val SPARK_SERIALIZER = params.getProperty(Constants.SPARK_SERIALIZER)
      //HBase 相关参数
    val HBASE_ADDRESS = params.getProperty(Constants.HBASE_ADDRESS)
    val HBASE_TABLE_TODAY_QUESTION = params.getProperty(Constants.HBASE_TABLE_TODAY_QUESTION)
    val HBASE_TABLE_WHOLE_QUESTION = params.getProperty(Constants.HBASE_TABLE_WHOLE_QUESTION)
    val HBASE_ZK_NODE = params.getProperty(Constants.HBASE_ZK_NODE)
      //Kafka 相关参数
    val KAFKA_BROKER_LIST = params.getProperty(Constants.KAFKA_BROKER_LIST)
    val KAFKA_GROUP_ID = params.getProperty(Constants.KAFKA_GROUP_ID_QUESTION_RECORD)
    val KAFKA_TOPIC = params.getProperty(Constants.KAFKA_TOPIC_SSDB)
    val KAFKA_ZOOKEEPER_QUORUM = params.getProperty(Constants.KAFKA_ZOOKEEPER_QUORUM)
    val topics = Array(KAFKA_TOPIC)
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> KAFKA_BROKER_LIST,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> KAFKA_GROUP_ID,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true : java.lang.Boolean),
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    )
    //2.创建spark,sc,ssc
      //创建SparkSession
    val spark = SparkSession.builder()
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME_QUESTION_RECORD)
        .getOrCreate()
      //创建SparkSession,设置日志级别
    val sc = spark.sparkContext
    sc.setLogLevel("warn")
      //创建SparkStreamingContext,设置检查点
    val ssc = new StreamingContext(sc, Durations.milliseconds(SPARK_DURATION_MILLISECONDS.toLong))
    ssc.checkpoint(SPARK_CHECKPOINT_DIR_QUESTION_RECORD)

    //3.连接kafka
    val kafkaStream = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    //4.业务代码，做RDD的转换
      //过滤作答未批改的答题记录
    val filterStream = kafkaStream.filter(line => {
        CommonUtils.isCorrect(line.value().toString)
      })
      //重分区，这里用partitionby来分区，由于其所需的类型是键值对RDD，所以这里需要先转换一下DStream类型
    val stream = filterStream.map(line => {
        val info = line.value()
        val studentId = CommonUtils.getStudentIdByQuestionLogInfo(info)
        (studentId, info)
      }).transform(rdd => {
        rdd.partitionBy(new HashPartitioner(3))
      })
      //缓存
    stream.persist(StorageLevel.MEMORY_AND_DISK)
      //创建数据库连接，将答题统计记录写入数据库（MySQL及HBase）
    stream.foreachRDD(rdd=>{
      rdd.foreachPartition(it=>{
        //创建MySQL 连接
        val writeJdbc = JDBCWriteUtils.getInstance(args(1))
        //创建HBase 连接
        val connection = new HBaseUtils(HBASE_ADDRESS, HBASE_ZK_NODE).getConnection
        //创建HBase 表连接
        val todayTable = connection.getTable(TableName.valueOf(HBASE_TABLE_TODAY_QUESTION))
        val wholeTable = connection.getTable(TableName.valueOf(HBASE_TABLE_WHOLE_QUESTION))
        while (it.hasNext){
          //获取value值，即一条日志
          val log = it.next()._2
          //打印日志
          logger.warn("收到一条日志："+log)
          //解析日志
          val info = CommonUtils.parseLogInfo(log)
          //写入数据
          QuestionUtils.saveQuestion(wholeTable,todayTable,writeJdbc,info)
        }
        //释放资源
        todayTable.close()
        wholeTable.close()
      })
    })
    //启动SparkStreaming程序
    ssc.start()
    ssc.awaitTermination()
  }
}
