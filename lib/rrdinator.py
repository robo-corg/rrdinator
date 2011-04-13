"""
rrd.py

Created by Nick Chen and Andrew McHarg on 2011-02-16.
Copyright (c) 2011 __MyCompanyName__. All rights reserved.
"""

import sys
import os
from subprocess import Popen, PIPE

from lxml import etree

class RRDExport:
    def __init__(self,src_rrd,xml_dom):
        self.src_rrd = src_rrd 
        self.xml_dom= xml_dom

    def step(self):
        int( self.xml_dom.xpath('/xport/meta/step')[0].text )

    def values(self):
        for node in self.xml_dom.xpath('/xport/data/row'):
            time_epoch = int( node.xpath('t')[0].text )

            if not (self.src_rrd.start == None or time_epoch >= self.src_rrd.start): continue
            if not (self.src_rrd.end   == None or time_epoch <= self.src_rrd.end ):  continue

            print [etree.tostring(value, pretty_print=True) for value in node.xpath('v')]

            yield tuple([time_epoch] + [float(value.text) for value in node.xpath('v')])

class Def:
    def __init__(self,name,value):
        self.name = name
        self.value = value

    def __str__(self):
        return 'DEF:%s=%s' % (self.name,self.value)

DEAULT_RRDTOOL = '/netops/rrdtool/1.4.2/bin/rrdtool'

class RRD:
    def __init__(self,rrdtool_exec=None):
        self.rrdtool_exec = rrdtool_exec
        self.args = []
        self.defs = []
        self.start = None
        self.end = None

    def start(self,time):
        self.start = int( mktime( starts.timetuple() ) )
        self.args += ['--start',start]
        return self

    def end(self,time):
        self.end = int( mktime( starts.timetuple() ) )
        self.args += ['--end',end]
        return self

    def add(self,_def):
        self.args.append(_def)
        self.defs.append(_def)
        return self

    def xport(self,*variables):
        ps = None
        export = None

        if len(variables) == 0:
            variables = [_def.name for _def in self.defs]

        xports = ['XPORT:'+var for var in variables]

        rrdtool_exec = DEAULT_RRDTOOL if self.rrdtool_exec == None else self.rrdtool_exec

        cmd_args = [rrdtool_exec, 'xport'] + map(str,self.args) + xports

        print ' '.join(cmd_args)

        try:
            ps = Popen( cmd_args, stdout=PIPE, stderr=None )        
            export = RRDExport(self,etree.parse( ps.stdout ))
        finally:
            ps.stdout.close()
            if ps.stdin != None:  ps.stdin.close()
            if ps.stderr != None: ps.stderr.close()

        return export
            
rrd = RRD()

rrd.add(Def('out_data','/data/poll/RRD/acar-ads-01/ifInErrors.108.rrd:data:AVERAGE'))

for row in rrd.xport().values():
    print row
