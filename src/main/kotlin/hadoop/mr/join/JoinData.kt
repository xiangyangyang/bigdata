package hadoop.mr.join

import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput

class JoinData : Writable {
    var flag: String = ""
    var dataText: String = ""
    override fun write(out: DataOutput?) {
        out?.let {
            it.writeUTF(flag)
            it.writeUTF(dataText)
        }
    }

    override fun readFields(input: DataInput?) {
        input?.let {
            flag = it.readUTF()
            dataText = it.readUTF()
        }
    }
}