#!/usr/local/bin/python2.7
# encoding: utf-8
'''
timberslide -- S3 to PostgreSQL batch load utility for Niddel-generated logs

@author:     https://github.com/asieira

@copyright:  2014 Niddel Corp and Seattle Children's Hospital. All rights reserved.

@contact:    contact@niddel.com
@deffield    updated: Updated
'''

import sys
import os
import logging

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from getpass import getpass
from time import time
from TimberSlide.slots import parseSlotRange, mergeSlotSets
from TimberSlide.s3repository import S3Repository, BZ2KeyIterator
from TimberSlide.db import connect, droptable, is_valid_id, createtable, insert
from TimberSlide.parse import TSVIterator
from multiprocessing import Process, JoinableQueue, cpu_count

__all__ = []
__version__ = 0.1
__date__ = '2014-12-23'
__updated__ = '2014-12-23'

DEBUG = 1
TESTRUN = 0
PROFILE = 0

class CLIError(Exception):
    '''Generic exception to raise and log different fatal errors.'''
    def __init__(self, msg):
        super(CLIError).__init__(type(self))
        self.msg = "E: %s" % msg
    def __str__(self):
        return self.msg
    def __unicode__(self):
        return self.msg
    
# This class is a process that gets S3 prefix names from a queue and writes the
# corresponding data to the database.    
class InserterProcess(Process):
    def __init__(self, name, queue, args):
        super(InserterProcess, self).__init__(name=name)
        self.args = args
        self.queue = queue
        self.daemon = True
        
    def run(self):
        conn = connect(self.args.server, self.args.database, self.args.user, 
                       self.args.password)
        repo = S3Repository(self.args.repository)
        logging.basicConfig(format='%(levelname)s %(asctime)s [%(processName)s] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S', level=logging.ERROR)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logger.info('started')

        while True:
            k = repo.get_prefix_key(self.queue.get())
            start = time()
            count = insert(conn, self.args.table, TSVIterator(BZ2KeyIterator(k)))
            end = time()
            logger.info('Inserted {} rows from {} in {} seconds'.format(str(count), k.name, 
                                                                         str(end-start)))
            self.queue.task_done()


def main(argv=None): # IGNORE:C0111
    '''Command line options.'''

    if argv is None:
        argv = sys.argv
    else:
        sys.argv.extend(argv)

    program_name = os.path.basename(sys.argv[0])
    program_version = "v%s" % __version__
    program_build_date = str(__updated__)
    program_version_message = '%%(prog)s %s (%s)' % (program_version, program_build_date)
    program_shortdesc = __import__('__main__').__doc__.split("\n")[1]
    program_license = '''%s

  Created by https://github.com/asieira on %s.
  Copyright 2014 Niddel Corp. and Seattle Children's Hospital. All rights reserved.

  Distributed on an "AS IS" basis without warranties
  or conditions of any kind, either express or implied.

''' % (program_shortdesc, str(__date__))

    try:
        # Setup argument parser
        parser = ArgumentParser(description=program_license, formatter_class=ArgumentDefaultsHelpFormatter)
        #parser.add_argument("-c", "--config", dest="config", help="path to the configuration file [default: %(default)s]", default="~/.timberslide")
        parser.add_argument('-v', '--version', action='version', version=program_version_message)
        parser.add_argument('-r', '--repository', default='s3://nevermind-logs/export/',
                            help='S3 directory where the data is located')
        parser.add_argument('-s', '--server', default='localhost:5432', 
                            help='PostgreSQL server host and port number as <host>[:<port>]')
        parser.add_argument('-d', '--database', type=is_valid_id, default='postgres', 
                            help='PostgreSQL database')
        parser.add_argument('-u', '--user', default='timberslide', help='PostgreSQL user name')
        parser.add_argument('-p', '--password', required=False,
                            help='PostgreSQL user password, if missing will be obtained interactively')
        parser.add_argument('-t', '--table', type=is_valid_id, default='logs', 
                            help='PostgreSQL table name to write to')
        parser.add_argument('-o', '--overwrite', action='store_true', 
                            help='if true, will delete any pre-existing table and create new prior to insertion')
        try:
            cpus = cpu_count()
        except NotImplementedError:
            cpus = 2
        parser.add_argument('-w', '--workers', type=int, default=cpus,
                            help='number of worker processes to use')
        parser.add_argument('slot', nargs='+', 
                            help='time slots or ranges of time slots to load, either <slot> or <slot>-<slot> for an inclusive range, <slot>- for all slots above and -<slot> for all slots below the provided one; each slot should be in YYYY, YYYYMM, YYYYMMDD or YYYYMMDDHH format (UTC)')

        # Process arguments
        args = parser.parse_args()
        
        # merge slots and give feedback
        logging.basicConfig(format='%(levelname)s %(asctime)s [%(processName)s] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S', level=logging.ERROR)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        repo = S3Repository(args.repository)
        args.slot = mergeSlotSets([parseSlotRange(s, repo) for s in args.slot])
        logger.info("Slots to process: " + ", ".join(sorted([str(s) for s in args.slot], 
                                                            reverse=True)))
        
        # find out all S3 keys to process
        keys = set()
        for s in args.slot:
            keys.update(repo.get_slot_keys(s))
        logger.info("Found "+str(len(keys))+" matching files at "+repo.location)

        # if password was not provided, get it interactively
        if args.password is None:
            args.password = getpass('Enter password for [{}@{}]: '.format(args.user, args.server))

        # delete and create SQL table if necessary
        conn = connect(args.server, args.database, args.user, args.password)
        if args.overwrite:
            logger.info('Dropping table \'{}\' if it exists...'.format(args.table))
            droptable(conn, args.table)
        createtable(conn, args.table)
        conn.commit()
        conn.close()

        # create queue and workers
        q = JoinableQueue()
        for i in range(args.workers):
            w = InserterProcess('Worker'+str(i), q, args)
            w.start()
        
        # add keys to queue and wait for workers to finish
        for k in keys:
            q.put(k.name)
        q.join()
        
        logger.info("All done!")

        return 0
    except KeyboardInterrupt:
        ### handle keyboard interrupt ###
        return 0
    except Exception, e:
        if DEBUG or TESTRUN:
            raise(e)
        indent = len(program_name) * " "
        logger.fatal(repr(e))
        sys.stderr.write(indent + "  for help use --help")
        return 2

if __name__ == "__main__":
#     if DEBUG:
#         sys.argv.append("-h")
#         sys.argv.append("-v")
    if TESTRUN:
        import doctest
        doctest.testmod()
    if PROFILE:
        import cProfile
        import pstats
        profile_filename = 'timberslide_profile.txt'
        cProfile.run('main()', profile_filename)
        statsfile = open("profile_stats.txt", "wb")
        p = pstats.Stats(profile_filename, stream=statsfile)
        stats = p.strip_dirs().sort_stats('cumulative')
        stats.print_stats()
        statsfile.close()
        sys.exit(0)
    sys.exit(main())