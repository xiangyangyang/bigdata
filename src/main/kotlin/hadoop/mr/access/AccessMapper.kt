package hadoop.mr.access

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class AccessMapper : Mapper<LongWritable, Text, Text, AccessData>(){
    override fun map(key: LongWritable?, value: Text?, context: Context?) {
        val values = value?.toString()?.split("\t")
        values?.let {
            val len = it.size
            val phone = it[1]
            val up = it[len - 3].toLong()
            val down = it[len -2].toLong()
            context?.write(Text(phone), AccessData(phone, up, down))
        }
    }
}