package org.keedio.hadoop.watcher


import java.io.{IOException}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileStatus}
import org.junit._
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/**
 * Created by luislazaro on 27/8/15.
 * lalazaro@keedio.com
 * Keedio
 */


class WatchableTest{

    val LOG: Logger = LoggerFactory.getLogger(classOf[WatchableTest])
    val hdfsConfig = new Configuration()
    val csvRegex: Regex = """[^.]*\.csv?""".r
    val anything: Regex = """.*""".r



    @Test
    def testGetCountFiles(): Unit = {
        println("##### testCountFiles: count number of files, via FileStatus vs ContentSummary")

        //count of files via watchable
        val watchable = new WatchablePath("src/test/resources/csv", hdfsConfig, 1, 0, anything)
        val countOfFiles_2= watchable.getFiles().size

        //count of files via contentSummary
        val path: Path = new Path("src/test/resources")
        val fs = path.getFileSystem(hdfsConfig)
        val arrayOfFileStatus: Array[FileStatus] = fs.listStatus(path)
        var countOfFiles_1: Int = 0
        arrayOfFileStatus.foreach(
            f => f.isDirectory match {
                case true => countOfFiles_1 += fs.getContentSummary(f.getPath).getFileCount.toInt
                case false => ()
            }
        )

        assert(countOfFiles_1 == countOfFiles_2, countOfFiles_1 + " != " + countOfFiles_2)

    }

    @Test
    def testGetCountFilesREGEX(): Unit = {
        println("##### testCountFilesREGEX: count number of files, via FileStatus vs ContentSummary")

        //count of files via watchable
        val watchable = new WatchablePath("src/test/resources/csv", hdfsConfig, 1, 0, csvRegex)
        val countOfFiles_2= watchable.getFiles().size

        //count of files via contentSummary
        val path: Path = new Path("src/test/resources")
        val fs = path.getFileSystem(hdfsConfig)
        val arrayOfFileStatus: Array[FileStatus] = fs.listStatus(path)
        var countOfFiles_1: Int = 0
        arrayOfFileStatus.foreach(
            f => f.isDirectory match {
                case true => countOfFiles_1 += fs.getContentSummary(f.getPath).getFileCount.toInt
                case false => ()
            }
        )

        assert(countOfFiles_1 != countOfFiles_2, countOfFiles_1 + " == " + countOfFiles_2)

    }


    @Test
    def testListOfFilesTime(): Unit = {
        println("##### testListOfFilesTime: test equality of list of mofified times (long) ")
        val watchable = new WatchablePath("src/test/resources/csv", hdfsConfig, 2, 2, csvRegex)
        val files: Array[FileStatus] = watchable.getFiles()
        val filesTimeList1: List[Long] = watchable.getTimeFiles(files)
        val filesTimeList2: List[Long] = watchable.getTimeFiles(files)
        assert(filesTimeList1 == filesTimeList2, filesTimeList1 + " != " + filesTimeList2)

        try {
            Files.write(Paths.get("src/test/resources/csv/file2.csv"), "224.0.0.0;8;Nicaragua\n".getBytes(), StandardOpenOption.APPEND);
        } catch {
            case e: IOException => LOG.error("I/O: testListOfFilesTime", e)
                assert(false)
        }

        val filesNow: Array[FileStatus] = watchable.getFiles()
        val filesTimeListNow: List[Long] = watchable.getTimeFiles(filesNow)
        assert(filesTimeList1 != filesTimeListNow, filesTimeList1 + " == " + filesTimeListNow)
    }

    @Test
    def testFireEvent(): Unit = {
        println("##### testFireEvent: all the status fire an event identified by the proper status ")
        val pathStatus: PathState = PathState.ENTRY_CREATE
        val watchable = new WatchablePath("src/test/resources/csv", hdfsConfig, 2, 2, csvRegex)

        val listener = new PathStateListener {
            override def statusReceived(event: PathStateEvent): Unit = {
                assert(pathStatus.toString.equals(event.getPathStatus.toString()))
            }
        }
        watchable.addEventListener(listener)
        watchable.fireEvent(pathStatus)
        watchable.removeEventListener(listener)
    }


    @Test
    def testLogicOfActions(): Unit = {
        println("##### testLogicOfActions: test logic of firing events according actions over the directory of csv files")
        var filesCount: Int = 0
        var filesTimeList: List[Long] = Nil

        val scheduler = Executors.newScheduledThreadPool(1)
        scheduler.scheduleAtFixedRate(checkDir(), 10, 2, TimeUnit.SECONDS)

        def checkDir(): Runnable = {
            new Runnable {
                override def run(): Unit = {
                    val path: Path = new Path("src/test/resources/csv")
                    val fs = path.getFileSystem(hdfsConfig)
                    val arrayOfFileStatus: Array[FileStatus] = fs.listStatus(path)
                    val files = arrayOfFileStatus.filter(_.isFile)
                    val filesCountNow = files.size
                    val filesTimeListNow = files.map(file => file.getModificationTime).toList

                    filesCount != filesCountNow match {

                        case true =>
                            filesCount < filesCountNow match {
                                case true =>
                                    assert(filesCount < filesCountNow)
                                case false =>
                                    assert(filesCount > filesCountNow)

                            }


                        //matching count of files but still check for modified files
                        case false =>
                            filesTimeList != filesTimeListNow match {
                                case true =>
                                    assert(filesTimeList != filesTimeListNow)
                                case false =>
                                    assert(filesTimeList == filesTimeListNow)
                            }
                    } //end of match
                    filesCount = filesCountNow
                    filesTimeList = filesTimeListNow
                }
            } // end of Runnable
        } //enf of check dir

        conditionsGenerator(10, 2000)
    } // end of test

    /**
     * For 20 seconds (10 iterations * 2 seconds) and every
     * 2 seconds, csv's directory will be checked. Each two iterations
     * and action will be taken over the files, i.e, delete file, append file,
     * create file. According the action a event will be fired.
     */
    @Test
    def testWatchPath(): Unit = {
        println("##### testWatchPath : watch directory a send events according actions  ")
        val refreshTime = 2
        val startTime = 2
        val watchable = new WatchablePath("src/test/resources/csv", hdfsConfig, refreshTime, startTime, csvRegex)
        val listener = new PathStateListener {
            override def statusReceived(event: PathStateEvent): Unit = {
                println("listener received event: " + event.getPathStatus.toString())
            }
        }
        watchable.addEventListener(listener)
        conditionsGenerator(10, 2000)  //(10 iterations * 2 seconds)
        watchable.removeEventListener(listener)
    }

    /**
     * Take actions over a directory to produce a response over time
     * @param iterations
     * @param timeToSleep
     */
    def conditionsGenerator(iterations: Int, timeToSleep: Long): Unit = {
        for (i <- 1 to iterations) {
            Thread.sleep(timeToSleep)
            println("iteration " + i)
            i match {
                case 3 =>
                    println("append to file")
                    try {
                        Files.write(Paths.get("src/test/resources/csv/file1.csv"),
                            "192.168.0.0;24;MOZAMBIQUE\n".getBytes(),
                            StandardOpenOption.APPEND)
                        Files.write(Paths.get("src/test/resources/csv/csv0/csv1/file5.csv"),
                            "Go back to Madrid\n".getBytes(),
                            StandardOpenOption.APPEND)
                    } catch {
                        case e: IOException => LOG.error("I/O: conditionsGenerator", e)
                            assert(false)
                    }

                case 5 =>
                    println("create new file")
                    try {
                        for (i <- 1 to 10)
                            Files.createFile(Paths.get(s"src/test/resources/csv/file_Created${i}.csv"))
                        for (i <- 1 to 5)
                            Files.createFile(Paths.get(s"src/test/resources/csv/csv0/file_Created${i + 1}.csv"))
                        for (i <- 1 to 5)
                            Files.createFile(Paths.get(s"src/test/resources/csv/csv0/file_Created${i + 1}.txt"))
                    } catch {
                        case e: IOException => LOG.error("I/O: conditionsGenerator", e)
                            assert(false)
                    }

                case 7 =>
                    println("delete file")
                    try {
                        for (i <- 1 to 10)
                            Files.deleteIfExists(Paths.get(s"src/test/resources/csv/file_Created${i}.csv"))
                        for (i <- 1 to 5)
                            Files.deleteIfExists(Paths.get(s"src/test/resources/csv/csv0/file_Created${i + 1}.csv"))
                        for (i <- 1 to 5)
                            Files.deleteIfExists(Paths.get(s"src/test/resources/csv/csv0/file_Created${i + 1}.txt"))
                    } catch {
                        case e: IOException => LOG.error("I/O: conditionsGenerator", e)
                            assert(false)
                    }

                case 10 => println("end")
                    try {
                        Files.deleteIfExists(Paths.get("src/test/resources/csv/file_Created.csv"))
                    } catch {
                        case e: IOException => LOG.error("I/O: conditionsGenerator", e)
                            assert(false)
                    }
                case _ => ()

            }

        }
    }

}