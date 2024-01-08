package hadoop.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.net.URI
import kotlin.jvm.Throws

/**
 * 使用javaAPI操作HDFS
 */
class HDFSApp {
    val HADOOP_URL = "hdfs://localhost:8020"
    val configuration = Configuration()
    lateinit var fileSystem: FileSystem
    @Before
    fun setup() {
        configuration.set("dfs.replication", "1")
        fileSystem=FileSystem.get(URI(HADOOP_URL), configuration)
    }

    @Test @Throws
    fun test1 ()  {
        fileSystem.mkdirs(Path("/","hdfs-test1"))
    }

    @Test
    fun test2() {
        val input = fileSystem.open(Path(("/vm_personal_license.txt")))
        IOUtils.copyBytes(input, System.out, 1024)
    }

    @Test
    fun test3() {
        val output = fileSystem.create(Path("/hdfs-api/test.txt"))
        output.writeBytes("hello hadoop test")
        output.flush()
        output.close()
    }

    @Test
    fun test4() {
        val input = BufferedInputStream(FileInputStream(File("/Users/chaixiangyang/Documents/hadoop/software/jdk-8u91-linux-x64.tar.gz")))
        val output = fileSystem.create(Path("/hdfs-api/jdk.tar")) {
            print(".")
        }
        IOUtils.copyBytes(input, output,4398)

    }

    @Test
    fun test5() {
        val fileStatus = fileSystem.getFileStatus(Path("/hdfs-api/jdk.tar"))
        val blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.len)
        for(x in blocks) {
            println("name is ${x.names[0]} , offset: ${x.offset} , length: ${x.length} ")
        }
    }

    @After
    fun tearDown() {

    }


}