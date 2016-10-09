#!/usr/bin/env python2.7
import threading
import Queue

class MyThread(threading.Thread):
    def __init__(self,stage_class,
                 inQ=None,outQs=None):
        threading.Thread.__init__(self)
        self._stage_class = stage_class
        self.inQ = inQ
        self.outQs = outQs

    def run(self):
        if self._stage_class is None:
            return
        #print 'MyThread.run(): vars()={}'.format(vars(self))
        envD = {
            'THREADNUM': int(self._Thread__name.split('-')[1]),
            'THREADNAME': self._Thread__name
        }
        stageObj = self._stage_class(envD,self.outQs)
        #
        # ----- if there is no input queu, just run the function
        # ----- with the runArgs given
        #
        #
        # ----- otherwise, call the function once for each packet
        # ----- in the input queue (and don't use runArgs)
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

        if self.outQs is not None:
            for outQ in self.outQs.values():
                if outQ is not None:
                    outQ.put('END')
            print '{}: sent END'.format(self._Thread__name)

        if hasattr(stageObj,'finish'):
            stageObj.finish()

class Stage(object):
    def __init__(self, stage_cls, inQ=None, outQs=None):
        self._class = stage_cls
        self._inQ = inQ
        self._outQs = outQs
        self._threads = []

    def start(self,nThreads=1):
        for ii in range(nThreads):
            t = MyThread(stage_class=self._class,
                         inQ=self._inQ,
                         outQs=self._outQs
            )
            self._threads.append( t)
            t.start()

    def wait(self):
        for t in self._threads:
            t.join()

    @classmethod
    def pipe(cls, *args):
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
