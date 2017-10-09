import logging
import threading
import time
from Queue import Queue, Empty


class ThreadPool(object):
    """
    Flexible thread pool class. Creates a pool of threads, then
    accepts tasks that will be dispatched to the next available
    thread.

    args:
        thread_num : the num of thread in the pool
        task_max_size : max size of the task queue
    """

    def __init__(self, thread_num, thread_local_constructors=None, task_max_size=0):
        """Initialize the thread pool with numThreads workers."""
        self.threads = []
        self.resize_lock = threading.Condition(threading.Lock())
        self.tasks = Queue(task_max_size)
        self.joining = False
        self.next_thread_id = 0
        assert thread_local_constructors is not None
        self.thread_local_constructors = thread_local_constructors
        self.set_thread_num(thread_num)

    def set_thread_num(self, new_thread_num):
        """
        External method to set the current pool size.
        Acquires the resizing lock, then calls the internal version 
        to do real work.
        """
        # Can't change the thread num if we're shutting down the pool!
        if self.joining:
            return False
        with self.resize_lock:
            self._set_thread_num_nolock(new_thread_num)
        return True

    def _set_thread_num_nolock(self, new_thread_num):
        """
        Set the current pool size, spawning or terminating threads if necessary.
        Internal use only;  assumes the resizing lock is held.
        """
        # If we need to expand the pool, do as down
        while new_thread_num > len(self.threads):
            new_thread = ThreadPoolThread(self, self.next_thread_id)
            self.next_thread_id += 1
            self.threads.append(new_thread)
            new_thread.start()
        # If we need to shrink the pool, do as below
        while new_thread_num < len(self.threads):
            self.threads[-1].go_away()
            del self.threads[-1]

    def get_thread_num(self):
        """Return the number of threads in the pool."""
        with self.resize_lock:
            return len(self.threads)

    def get_task_num(self):
        """Return the number of tasks in the pool."""
        return self.tasks.qsize()

    def queue_task(self, task, args, callback=None):
        """
        Insert a task into the queue. task must be callable;
        args and taskCallback can be None.
        """
        if self.joining:
            return False
        if not callable(task):
            return False
        self.tasks.put((task, args, callback))
        return True

    def get_next_task(self):
        """
        Retrieve the next task from the task queue.
        For use only by ThreadPoolThread objects contained in the pool.
        """
        try:
            return self.tasks.get_nowait()
        except Empty:
            return None, None, None

    def join_all(self, wait_for_tasks=True, wait_for_threads=True):
        """
        Clear the task queue and terminate all pooled threads,
        optionally allowing the tasks and threads to finish.
        """
        # Mark the pool as joining to prevent any more task queueing
        self.joining = True
        #  Wait for tasks to finish
        if wait_for_tasks:
            while not self.tasks.empty():
                time.sleep(1)
        # Inotify all the threads to quit
        with self.resize_lock:
            if wait_for_threads:
                for t in self.threads:
                    t.go_away()
                for t in self.threads:
                    # wait for thread execute to the end
                    t.join()
                    del t
            self._set_thread_num_nolock(0)
            # Reset the pool for potential reuse
            self.joining = False


class ThreadPoolThread(threading.Thread):
    def __init__(self, pool, id):
        """ Initialize the thread and remember the pool. """
        threading.Thread.__init__(self)
        # exit when the main thread exit
        self.setDaemon(True)
        self.pool = pool
        self.running = False
        self.locals = {'thread_id': id}
        for name, (constructor, args) in pool.thread_local_constructors.items():
            self.locals[name] = constructor(*args)

    def run(self):
        """
        Retrieve the next task and execute it, call the callback if any, 
        until receive the quit signal
        """
        self.running = True
        while self.running:
            try:
                cmd, args, callback = self.pool.get_next_task()
                if cmd is None:
                    time.sleep(0.2)
                    continue
                else:
                    # args must be multi arg, only one arg, it may error, special in the only arg type is str
                    res = cmd(*args, **self.locals)
                if callback:
                    res = callback(res, **self.locals)
            except Exception, e:
                logging.exception(e)

    def go_away(self):
        """ Exit the run loop next time through. """
        self.running = False
