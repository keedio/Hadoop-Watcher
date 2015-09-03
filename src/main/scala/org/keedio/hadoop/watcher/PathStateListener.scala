package org.keedio.hadoop.watcher

/**
 * Created by luislazaro on 26/8/15.
 * lalazaro@keedio.com
 * Keedio
 */

/**
 * Interface for PathStateEvent to inform other about
 * its status. When PathHdfsObject changes its status,
 * he will call the statuReceived method on each of his
 * listeners
 */

trait PathStateListener {
    def statusReceived(event: PathStateEvent):Unit
}
