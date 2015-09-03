package org.keedio.hadoop.watcher

/**
 * Created by luislazaro on 26/8/15.
 * lalazaro@keedio.com
 * Keedio
 */

/**
 * PathHdfsObject has three states. When he fires an event, the listener
 * will retrieve one of them.
 * @param name
 */
class PathState(name: String) {
    override def toString(): String = {
        name
    }
}

object PathState extends Serializable{
    final val ENTRY_CREATE: PathState = new PathState("entry_create")
    final val ENTRY_DELETE: PathState = new PathState("entry_delete")
    final val ENTRY_MODIFY: PathState = new PathState("entry_modify")
}
