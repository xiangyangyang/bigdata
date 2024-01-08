package hadoop.mr

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class WCMapper : Mapper<LongWritable, Text, Text, IntWritable>() {

    override fun map(key: LongWritable?, value: Text?, context: Context?) {
        val keys = value.toString().split(" ")
        for (x in keys) {
            context?.write(Text(x), IntWritable(1))
        }
    }
}