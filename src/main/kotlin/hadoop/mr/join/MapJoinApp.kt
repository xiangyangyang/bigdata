package hadoop.mr.join

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import java.io.BufferedReader
import java.io.FileReader
import java.net.URI

fun main(args: Array<String>) {
    MapJoinApp()
}
class MapJoinApp {
    init {
        val job = Job.getInstance()
        job.setJarByClass(MapJoinApp::class.java)
        job.mapperClass = MapJoinMapper::class.java

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = NullWritable::class.java
        job.numReduceTasks = 0

        job.addCacheFile(URI("input/join/dept.txt"))
        val fileSystem = FileSystem.get(Configuration())
        val outPath = Path("output/mapjoin/")
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true)
        }
        FileInputFormat.setInputPaths(job, Path("input/join/emp.txt"))
        FileOutputFormat.setOutputPath(job, outPath)

        job.waitForCompletion(true)
    }
    class MapJoinMapper : Mapper<LongWritable, Text, Text, NullWritable> () {
        var cache: HashMap<String, String> = HashMap()
        override fun setup(context: Context?) {
            val path = context!!.cacheFiles[0].path
            val bufferReader = BufferedReader(FileReader(path))
            var line = bufferReader.readLine()
            while (line != null) {
                val splits = line.split("\t")
                cache[splits[0]] = splits[1]
                line = bufferReader.readLine()
            }
        }

        override fun map(key: LongWritable?, value: Text?, context: Context?) {
            if(value == null) return
            val values = value.toString().split("\t")
            if(values.size == 8) {
                val builder = StringBuilder()
                val deptNo = values[7]
                builder.append(values[0]).append("\t").append(values[1])
                    .append("\t").append(values[2])
                    .append("\t").append(values[5])
                    .append("\t").append(values[7])
                    .append("\t").append(cache.get(deptNo))
                val key = builder.toString()
                context?.write(Text(key), NullWritable.get())
            }
        }
    }
}