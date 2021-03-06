package org.keedio.hadoop.watcher


import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit, Executors}
import scala.collection.mutable.ListBuffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import scala.util.matching.Regex


/**
 * Created by luislazaro on 25/8/15.
 * lalazaro@keedio.com
 * Keedio
 */

/**
 * The event generator (event source)
 * @param csvDir
 * @param hdfsConfig
 * @param refresh
 */

class WatchablePath(csvDir: String, hdfsConfig: Configuration, refresh: Int, start: Int, regex: Regex ){

    private var filesCount: Int = 0
    private var filesTimeList: List[Long] = Nil
    private val listeners: ListBuffer[PathStateListener] = new ListBuffer[PathStateListener]

    //private val startWatching = 2

    private val corePoolSize = 1 // the number of threads to keep in the pool, even if they are idle
    private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(corePoolSize)
    private val tasks: ScheduledFuture[_] = scheduler.scheduleAtFixedRate(getTaskToSchedule(),
        start, refresh, TimeUnit.SECONDS)

    /**
     * Make a method runnable and schedule for
     * periodic execution
     * @return
     */
    def getTaskToSchedule(): Runnable = {
        new Runnable {
            override def run(): Unit = {
                watchPath()
            }
        }
    }

    /**
     * Check hdfs path for changes
     */
    def watchPath(): Unit = {
        val filesNow: Array[FileStatus] = getFiles()
        val filesCountNow: Int = getCountFiles(filesNow)
        val filesTimeListNow: List[Long] = getTimeFiles(filesNow)

        filesCount != filesCountNow match {

            case true => {
                filesCount < filesCountNow match {
                    case true => {
                        fireEvent(PathState.ENTRY_CREATE)
                    }
                    case false => {
                        fireEvent(PathState.ENTRY_DELETE)
                    }
                }
            }
            //matching count of files but still check for modified files
            case false => filesTimeList != filesTimeListNow match {
                case true => fireEvent(PathState.ENTRY_MODIFY)
                case false => ()
            }
        }

        filesCount_=(filesCountNow)
        filesTimeList_=(filesTimeListNow)
    }



    /**
     * Add element to list of registered listeners
     * @param listener
     */
    def addEventListener(listener: PathStateListener): Unit = {
        listener +=: listeners
    }

    /**
     * Remove element from list of registered listeners
     * @param listener
     */
    def removeEventListener(listener: PathStateListener): Unit = {
        listeners.find(_ == listener) match {
            case Some(listener) => {
                listeners.remove(listeners.indexOf(listener))
            }
            case None => ()
        }
    }


    /**
     * call this method whenever you want to notify the event listeners about a particular event
     */
    def fireEvent(pathStatus: PathState): Unit = {
        val event: PathStateEvent = new PathStateEvent(this, pathStatus)
        listeners foreach (_.statusReceived(event))
    }


    /**
     * Get an Array of Filestatus as Files in the directories tree
     * @return
     */
    def getFiles(): Array[FileStatus] = {
        val path: Path = new Path(csvDir)
        val fs = path.getFileSystem(hdfsConfig)
        val arrayOfFileStatus: Array[FileStatus] = fs.listStatus(path)
        val a: Array[Array[FileStatus]] = arrayOfFileStatus.map(fileStatus => getRecursiveListFiles(fileStatus, regex ))
        a.flatMap(_.toList).filter(_.isFile)
    }

    /**
     * Count of files in hadoop path
     * @param files
     * @return
     */
    def getCountFiles(files: Array[FileStatus]): Int = {
        files.size
    }

    /**
     * Get list of file's modification times
     * @param files
     * @return
     */
    def getTimeFiles(files: Array[FileStatus]): List[Long] = {
        files.map(file => file.getModificationTime).toList
    }

    /**
     * Get an array of files in FileStatus as directory recursively
     * @param fileStatus
     * @return
     */
    def getRecursiveListFiles(fileStatus:FileStatus, regex: Regex): Array[FileStatus] = {
        val path : Path = fileStatus.getPath
        val fs = path.getFileSystem(hdfsConfig)
        val arrayOfFileStatus = fs.listStatus(path)
        val matches = arrayOfFileStatus.filter(fileStatus => regex.findFirstIn(fileStatus.getPath.toString).isDefined)
        matches ++ arrayOfFileStatus.filter(_.isDirectory).flatMap(getRecursiveListFiles(_,regex))
    }

}
