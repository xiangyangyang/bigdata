package hadoop.mr.access

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

class AccessReducer:Reducer<Text, AccessData, Text, AccessData>() {
    override fun reduce(key: Text?, values: MutableIterable<AccessData>?, context: Context?) {
        if (values == null || key == null) return
        var up = 0L
        var down = 0L
        val it = values.iterator()
        while (it.hasNext()) {
            val accessData = it.next()
            up += accessData.up
            down += accessData.down
        }
        context?.write(key, AccessData(key.toString(), up, down))
    }
}