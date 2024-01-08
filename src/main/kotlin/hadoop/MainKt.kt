package hadoop

import hadoop.mr.WCDriver
import hadoop.mr.access.AccessLocalDriver
import hadoop.mr.access.AccessYarnDriver

fun main(args: Array<String>) {
//    val hdfswC001 = HDFSWC001()
//    hdfswC001.wordCount()

    AccessYarnDriver(args)
//    AccessLocalDriver()

}