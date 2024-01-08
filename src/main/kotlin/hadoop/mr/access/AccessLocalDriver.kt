package hadoop.mr.access

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class AccessLocalDriver {
    init {
        val job = Job.getInstance()
        job.setJarByClass(AccessLocalDriver::class.java)
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
        val outPath = Path("access/output/")
        if(fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true)
        }
        FileInputFormat.setInputPaths(job, Path("access/input/access.log"))
        FileOutputFormat.setOutputPath(job, Path("access/output/"))

        job.waitForCompletion(true)
    }
}