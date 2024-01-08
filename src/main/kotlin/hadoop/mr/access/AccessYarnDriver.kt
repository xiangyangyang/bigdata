package hadoop.mr.access

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class AccessYarnDriver(args: Array<String>) {
    init {
        val job = Job.getInstance()
        job.setJarByClass(AccessYarnDriver::class.java)
        job.mapperClass = AccessMapper::class.java
        job.combinerClass = AccessReducer::class.java
        job.reducerClass = AccessReducer::class.java

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = AccessData::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = AccessData::class.java

        job.partitionerClass = AccessPartitioner::class.java
        job.numReduceTasks = 3

        val fileSystem = FileSystem.get(Configuration())
        val outPath = Path(args[1])
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true)
        }
        FileInputFormat.setInputPaths(job, Path(args[0]))
        FileOutputFormat.setOutputPath(job, Path(args[1]))

        job.waitForCompletion(true)
    }
}