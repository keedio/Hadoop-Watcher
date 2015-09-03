package org.keedio.hadoop.watcher

import java.util.EventObject

/**
 * Created by luislazaro on 26/8/15.
 * lalazaro@keedio.com
 * Keedio
 */

/**
 * Event class
 * PathStateEvent holds on the source
 * @param source
 */
class PathStateEvent(source: Object, pathStatus: PathState) extends EventObject(source){

    def getPathStatus = pathStatus

}
