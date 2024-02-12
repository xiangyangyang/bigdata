package hadoop.mr.join

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

fun main(args: Array<String>) {
    ReduceJoinApp()
}

class ReduceJoinApp {
    init {

        val job = Job.getInstance()
        job.setJarByClass(ReduceJoinApp::class.java)
        job.mapperClass = JoinMapper::class.java
        job.reducerClass = JoinReducer::class.java

        job.mapOutputKeyClass = Text::class.java
        job.mapOutputValueClass = JoinData::class.java
        job.outputKeyClass = Text::class.java
        job.outputValueClass = NullWritable::class.java

        val fileSystem = FileSystem.get(Configuration())
        val outPath = Path("output/join/")
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true)
        }
        FileInputFormat.setInputPaths(job, Path("input/join/"))
        FileOutputFormat.setOutputPath(job, outPath)

        job.waitForCompletion(true)
    }

    class JoinMapper : Mapper<LongWritable, Text, Text, JoinData>() {
        override fun map(key: LongWritable?, value: Text?, context: Context?) {
            if (value == null) return
            val values = value.toString().split("\t")
            val data = JoinData()
            if (values.size == 3) {
                data.flag = "d"
                data.dataText = values[1]

                context?.write(Text(values[0]), data)
            } else if (values.size == 8) {
                data.flag = "e"
                val builder = StringBuilder()
                builder.append(values[0]).append("\t").append(values[1])
                    .append("\t").append(values[2])
                    .append("\t").append(values[5])
                    .append("\t").append(values[7])
                data.dataText = builder.toString()

                context?.write(Text(values[7]), data)
            }
        }
    }

    class JoinReducer : Reducer<Text, JoinData, Text, NullWritable>() {
        override fun reduce(key: Text?, values: MutableIterable<JoinData>?, context: Context?) {
            if (values == null) return
            val empList = mutableListOf<String>()
            var dept = ""
            val iterator = values.iterator()
            while (iterator.hasNext()) {
                val data = iterator.next()
                if (data.flag == "d") {
                    dept = data.dataText
                } else {
                    empList.add(data.dataText)
                }
            }

            for (i in empList.indices) {
                context?.write(Text(empList[i] + "\t" + dept), NullWritable.get())
            }

        }

    }
}