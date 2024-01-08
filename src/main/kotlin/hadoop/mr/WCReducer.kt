package hadoop.mr

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class WCReducer : Reducer<Text,IntWritable, Text, IntWritable>() {
    override fun reduce(key: Text?, values: MutableIterable<IntWritable>?, context: Context?) {
        if (values == null) return
        val it = values.iterator()
        var count = 0
        while (it.hasNext()) {
            count += it.next().get()
        }
        context?.write(key, IntWritable(count))
    }
}