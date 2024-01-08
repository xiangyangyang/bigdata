package org.myapp.bigdata.hadoop.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.FileReader
import java.io.InputStreamReader
import java.net.URI

/**
 * word count
 */
class HDFSWC001 {
    val HADOOP_URL = "hdfs://localhost:8020"
    val configuration = Configuration()
    lateinit var fileSystem: FileSystem
   init {
       configuration.set("dfs.replication", "1")
       fileSystem=FileSystem.get(URI(HADOOP_URL), configuration)
   }

    fun wordCount() {
        // read hdfs file
        val inputStream = fileSystem.open(Path("/hdfs-api/test/word.txt"))
        val bufferReader = BufferedReader(InputStreamReader(inputStream, "UTF-8"))
        // split to list
        val result = mutableListOf<String>()
        var line = bufferReader.readLine()
        while (line != null) {
            result.addAll(line.split(" "))
            line = bufferReader.readLine()
        }
        // put into map
        val resultMP = mutableMapOf<String, Int>()
        for(w in result) {
            if(resultMP.containsKey(w)) {
                var count = resultMP.get(w) ?: 0
                count += 1
                resultMP.set(w, count)
            } else {
                resultMP.put(w, 1)
            }
        }

        // write to result file
        val outputFile = fileSystem.create(Path("/hdfs-api/test/wordCount.txt"), true)
        val it = resultMP.iterator()
        while (it.hasNext()){
            val map = it.next()
            outputFile.writeChars(map.key + "," + map.value + "\n")
        }

        outputFile.flush()
        outputFile.close()
    }
}