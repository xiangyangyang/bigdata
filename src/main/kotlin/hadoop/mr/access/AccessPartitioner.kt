package hadoop.mr.access

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Partitioner

class AccessPartitioner : Partitioner<Text, AccessData>() {
    override fun getPartition(key: Text?, value: AccessData?, numPartitions: Int): Int {
        key?.let {
            val phone = it.toString()
            if (phone.startsWith("13")) {
                return 0
            } else if (phone.startsWith("15")) {
                return 1
            }
        }
        return 2
    }
}