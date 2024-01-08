package hadoop.mr.access

import org.apache.hadoop.io.Writable
import java.io.DataInput
import java.io.DataOutput

class AccessData: Writable {
    var phoneNumber: String = ""
    var up: Long = 0
    var down: Long = 0
    private var sum: Long = 0
    constructor() {

    }

    constructor(_phoneNumber: String, _up: Long, _down: Long) {
        phoneNumber = _phoneNumber
        up = _up
        down = _down
        sum = up + down
    }

    override fun toString(): String {
        return "$up\t$down\t$sum"
    }

    override fun write(out: DataOutput?) {
        out?.let {
            it.writeUTF(phoneNumber)
            it.writeLong(up)
            it.writeLong(down)
            it.writeLong(sum)
        }
    }

    override fun readFields(`in`: DataInput?) {
        `in`?.let {
            phoneNumber = it.readUTF()
            up = it.readLong()
            down = it.readLong()
            sum = it.readLong()
        }
    }

}