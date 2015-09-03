# Hadoop Watcher

## Main goal
Keedio's Hadoop Watcher is a functionality for watching hdfs paths for changes. When a change occurs in the observed directory a event is fired. All listeners subscribed to the event generator will be notified.

## How To Use
Normally a object interested in events (listener) from a path to be watched should instance WatchablePath with:

* String path to be watched.
* Hadoop config.
* Start time in seconds.
* Periodic time in seconds.
* Regexp for matching files.

The listener will have to register himself to the watchable object and will implement what to do, when something happens in the monitored path.

The path will be checked periodically for new files or changes in its modification times.
If the path contains subpaths, changes under subdirectories will all be watched.


