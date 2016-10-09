#!/usr/bin/env python2.7
import os
import sys
import os.path

import json
import StringIO
import tarfile
from collections import Counter

from stage import Stage

class ListDir(object):
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
    tgz_fname = None
    def __init__(self,envD,outQs):
        self.catD = Counter()
        self.tgz_fname = Categorize.tgz_fname.format(**envD)
        print 'Categorize.__init__(tgz_fname={})'.format(self.tgz_fname)

    def per_item(self,q_item):
        base,ext = os.path.splitext(q_item)
        self.catD[ext] += 1

    def finish(self):
        tgz = tarfile.open(self.tgz_fname,'w:gz')
        tarinfo = tarfile.TarInfo('test')
        jsonS = json.dumps(self.catD)
        stringfp = StringIO.StringIO()
        stringfp.write(jsonS)
        stringfp.seek(0)
        tarinfo.size = len(stringfp.buf)
        tgz.addfile(tarinfo=tarinfo,fileobj=stringfp)
        tgz.close()

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
    Categorize.tgz_fname = 'info_{THREADNUM}.tgz'

    stages = Stage.pipe( [ListDir], [ListTar]*3, [Categorize]*2)
    for s in stages:
        s.wait()

    print 'DONE'
