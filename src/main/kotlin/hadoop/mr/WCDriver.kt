package hadoop.mr

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import kotlin.system.exitProcess

class WCDriver {
//    val HADOOP_URL = "hdfs://localhost:8020"
//    val configuration = Configuration()
    init {
//        configuration.set("fs.defaultFS", HADOOP_URL)
//        val job = Job.getInstance(configuration)
        // 本地模式运行
        val job = Job.getInstance()
        job.setJarByClass(WCDriver::class.java)
        job.mapperClass = WCMapper::class.java
        // mapper端端聚合操作能减少数据量传输，因为通常map，reduce操作在不同端datanode上
        job.combinerClass = WCReducer::class.java
        job.reducerClass = WCReducer::class.java

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = IntWritable::class.java

        job.outputKeyClass = Text::class.java
        job.outputValueClass = IntWritable::class.java

        // 本地模式运行
        FileInputFormat.setInputPaths(job, Path("input/wc.input"))
        FileOutputFormat.setOutputPath(job, Path("output"))

        val result = if (job.waitForCompletion(true)) 0 else -1
        exitProcess(result)

    }

}