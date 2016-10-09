#!/usr/bin/env python2.7
import threading
import Queue

class MyThread(threading.Thread):
    def __init__(self,stage_class,
                 inQ=None,outQs=None):
        """
        initialize with the input and output queues and the class
        of object that will execute in this thread
        """
        threading.Thread.__init__(self)
        self._stage_class = stage_class
        self.inQ = inQ
        self.outQs = outQs

    def run(self):
        """
        this is called when the thread starts, and the thread continues
        until this method returns. This creates the object that will run
        in the thread and calls it for every packet that arrives in the input
        queue.
        If there is no input queue, the object is assumed to do all its
        work in its constructor.
        When the packet from the queue == 'END', the thread will exit, but
        it will first push the 'END' back on the *input* queue, in case any
        other threads are also puling from this queue
        """
        #
        # ----- create the processing object
        #
        if self._stage_class is None:
            return
        #print 'MyThread.run(): vars()={}'.format(vars(self))
        envD = {
            'THREADNUM': int(self._Thread__name.split('-')[1]),
            'THREADNAME': self._Thread__name
        }
        stageObj = self._stage_class(envD,self.outQs)
        #
        # ----- if there is an input queue
        # -----    send each packet from the queue to the objects
        # -----    per_item() method
        # -----    when the packet == 'END', push it back to the queue
        # -----    and exit
        #
        if self.inQ is not None:
            while True:
                packet = self.inQ.get()
                print '{}: received {}'.format(self._Thread__name,packet)
                if packet == 'END':
                    self.inQ.put(packet)
                    print '{}: pushed back {}'.format(self._Thread__name,packet)
                    break
                if self.outQs is not None:
                    stageObj.per_item(packet)
        #
        # ----- before exiting, push the 'END' packet to to output queue(s)
        #
        if self.outQs is not None:
            for outQ in self.outQs.values():
                if outQ is not None:
                    outQ.put('END')
            print '{}: sent END'.format(self._Thread__name)
        #
        # ----- if the object has a finish() method, call it before exiting
        #
        if hasattr(stageObj,'finish'):
            stageObj.finish()

class Stage(object):
    def __init__(self, stage_cls, inQ=None, outQs=None):
        """
        create a stage object, which will create multiple threads,
        each running an object of the class provided in "stage_cls".
        This object can received packets from and input queue, send
        packets to output queue(s) or both
        """
        self._class = stage_cls
        self._inQ = inQ
        self._outQs = outQs
        self._threads = []

    def start(self,nThreads=1):
        """
        start multiple instances of the object for this stage, each
        on its own thread
        """
        for ii in range(nThreads):
            t = MyThread(stage_class=self._class,
                         inQ=self._inQ,
                         outQs=self._outQs
            )
            self._threads.append( t)
            t.start()

    def wait(self):
        """
        wait for all the threads in this stage to complete
        """
        for t in self._threads:
            t.join()

    @classmethod
    def pipe(cls, *args):
        """
        convenience method to create a pipeline of parallel stages, each running
        one or more threads. The stages are in a pipeline, where the first stage
        writes to a queue that is read by the second stage, which writes to a
        queue read by the third stage, etc.

        Example call:

        stages = Stage.pipe([cls_one],[cls_two]*10,[cls_three]*2)

        will create 3 stages. The first stage will have a single thread containing
        an object of class "cls_one". The second stage will have 10 threads, each
        with an object of class "cls_two" and the third stage will have 2 threads
        of "cls_three" objects.

        Each class must have the following interface:
        	__init__(self, envD, outQs)
        	per_item(self,packet)		(optional)
	        finish(self)			(optional)

        the init get a dict of "environmen variables" it can access in a
        threadsave manner. Examples are THREADNAME and THREADNUM.
        Additionly, it gets a dictionary of output queues it can write to. This
        can be None, if it is not to write to a queue.

        per_item() is called once per item pulled from the input queue (if any)
        this method should process that item and write the result to the output
        queue (or queues)

        if the object needs any final processing before it exits (such as closing
        files), it should implement a finish() method
        """
        print '{} stages'.format(len(args))
        #
        # ----- create n-1 queues, where n is the number of
        # ----- stages defined
        #
        Qs = []
        for ii in range(len(args)-1):
            Qs.append(Queue.Queue())
        #
        # ----- foreach stage, define the class to execute,
        # ----- the input and output queues
        #
        stages = []
        for nstage, arg in enumerate(args):
            stage_cls = arg[0]
            inQ = None if nstage == 0 else Qs[nstage-1]
            outQ = None if nstage == len(Qs) else Qs[nstage]
            print 'createing Stage for {}'.format(stage_cls)
            stage = Stage(stage_cls,inQ=inQ,outQs={'out':outQ})
            stages.append(stage)
        #
        # ----- kick them all off, with the proper number
        # ----- of threads in each stage
        #
        for nstage, arg in enumerate(args):
            nthreads = len(arg)
            stages[nstage].start(nThreads=nthreads)
        return stages
