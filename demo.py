#!/usr/bin/env python2.7
"""Example of using the Stage classes for parallel pipelined processing.

This example has 3 stages.

Stage 1 reads from a directory to get a list of tar.gz files. It writes those
filenames to an output queue.

Stage 2 reads the names of the tar.gz files from that queue, and for
each one, it extracts the list of filenames from it. It writes these
filenames to the next output queue. In this example, we run 3 of these
threads in parallel.

Stage 3 reads the names of the tar member files and pulls off the file
extension.  It uses this to build a count of how many files of each
file extension exist. This example runs 2 of these threads. When all
the member filenames have been read, the object writes the count
dictionary to another tar.gz file. Since 2 threads are running, we end
up with 2 output files, info_5.tgz and info_6.tgz.

All total, this example will run 6 simultaneous threads and exit only when
they are all finished.

"""
import os
import sys
import os.path

import json
import StringIO
import tarfile
from collections import Counter

from stage import Stage

class ListDir(object):
    """
    The first stage does not read from and input queue. Instead it reads
    from the directory given in the class member "dirname". Because it does
    not read from a queue, or have any cleanup processing, only a __init__
    method is required.
    """
    dirname = None
    def __init__(self,envD,outQs):
        print 'ListDir.__init__({})'.format(ListDir.dirname)
        for fname in os.listdir(ListDir.dirname):
            if not fname.endswith('gz'):
                continue
            path = os.path.join(ListDir.dirname,fname)
            outQs.values()[0].put(path)
            print 'ListDir.put({})'.format(path)

class ListTar(object):
    """
    ListTar receives the name of a tar.gz file from a queue and lists
    out the names of its members. It writes these names to the output
    queue in the "per_item()" method.
    """
    def __init__(self,envD,outQs):
        self._outQs = outQs

    def per_item(self,q_item):
        print '--> ListTar.per_item({})'.format(q_item)

        tar = tarfile.open(q_item,'r:gz')
        for member in tar.getmembers():
            packet = '{}:{}'.format(q_item, member.name)
            self._outQs.values()[0].put(packet)
        print '<-- ListTar.per_item()'

class Categorize(object):
    """
    Categorize gets the member filename and splits off the file extension
    and counts how many of each it receives. When it is done, it creates
    a new file containing the count dictionary in json format.
    """
    count_fname = None
    def __init__(self,envD,outQs):
        self.catD = Counter()
        self.count_fname = Categorize.count_fname.format(**envD)
        print 'Categorize.__init__(count_fname={})'.format(self.count_fname)

    def per_item(self,q_item):
        base,ext = os.path.splitext(q_item)
        self.catD[ext] += 1

    def finish(self):
        with open(self.count_fname,'w') as fp:
            json.dump(self.catD, fp, sort_keys=True, indent=4)

if __name__ == '__main__':
    #
    # -------------       -------------       ----------------
    # | stage1    |       |  stage2   |       | stage3       |
    # |-----------|       |-----------|       |--------------|
    # | listdir() | ----> | listtar() | ----> | categorize() |
    # |   *1      |       |    *3     |       |      *2      |
    # -------------       -------------       ----------------
    #
    ListDir.dirname = '/Users/gardner/Work/IBM/IBM_CODE_PRE_JUNE_2007'
    Categorize.count_fname = 'count_{THREADNUM}.json'

    stages = Stage.pipe( [ListDir], [ListTar]*3, [Categorize]*2)
    for s in stages:
        s.wait()

    print 'DONE'
