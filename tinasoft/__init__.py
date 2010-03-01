# -*- coding: utf-8 -*-
__author__="Elias Showk"
__all__ = ["pytextminer","data"]

# tinasoft core modules
from tinasoft.pytextminer import stopwords, indexer, tagger, tokenizer, \
    corpora, ngram, cooccurrences
from tinasoft.data import Engine, Reader, Writer

# checks or creates aaplication directories
from os import makedirs
from os.path import exists, join
if not exists('log'):
    makedirs('log')
if not exists('index'):
    makedirs('index')
if not exists('db'):
    makedirs('db')

# locale management
import locale
# command line utility
from optparse import OptionParser
# configuration file parsing
import yaml

# logger
import logging
import logging.handlers

# json
import jsonpickle

# Threads
from time import sleep
import threading

class DBInconsistency(Exception): pass

class TinaApp():
    """ base class for a tinasoft.pytextminer application"""
    def __init__(
        self,
        configFile='config.yaml',
        storage=None,
        loc=None,
        stopw=None,
        index=None):

        # import config yaml
        try:
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            return

        # Set up a specific logger with our desired output level
        self.LOG_FILENAME = self.config['log']
        # logger config
        logging.basicConfig(filename=self.LOG_FILENAME,
            level=logging.DEBUG,
            datefmt='%Y-%m-%d %H:%M:%S',
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # Add the log message handler to the logger
        rotatingFileHandler = logging.handlers.RotatingFileHandler(
            filename=self.LOG_FILENAME,
            maxBytes=100000000, backupCount=5
        )
        # formatting
        formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
        rotatingFileHandler.setFormatter(formatter)
        self.logger = logging.getLogger('TinaAppLogger')
        # set default level to DEBUG
        self.logger.setLevel(logging.DEBUG)

        #self.logger.addHandler(handler)
        # tries support of the locale by the host system
        try:
            if loc is None:
                self.locale = self.config['locale']
            else:
                self.locale = loc
            locale.setlocale(locale.LC_ALL, self.locale)
        except:
            self.locale = 'en_US.UTF-8'
            self.logger.error( "locale %s was not found,\
                switching to en_US.UTF-8 by default"%self.locale)
            locale.setlocale(locale.LC_ALL, self.locale)


        options = {
            'home' : self.config['dbenv']
        }
        # connection to storage
        if storage is None:
            self.storage = Engine(self.config['storage'], **options)
        else:
            self.storage = Engine(storage)

        # connect to text-index
        if index is None:
            self.index = indexer.TinaIndex(self.config['index'])
        else:
            self.index = indexer.TinaIndex(index)
        self.logger.debug("Starting TinaApp")

    def serialize(self, obj):
        return jsonpickle.encode(obj)

    def deserialize(self, str):
        return jsonpickle.decode(str)

    def commitAll(self):
        # wait for transactions to finish
        self.storage.closeAllTxn(commit_pending_transaction=True, commit_root=False)
        self.storage.closeAllTxn(commit_pending_transaction=True, commit_root=True)

    def importFile(self,
            path,
            configFile,
            corpora_id,
            overwrite=False,
            index=False,
            format= 'tina',
        ):
        """tina file import method"""
        try:
            # import import config yaml
            self.config = yaml.safe_load( file( configFile, 'rU' ) )
        except yaml.YAMLError, exc:
            self.logger.error( "Unable to read the importFile special config : " + exc)
            return
        # load Stopwords object
        self.stopwords = stopwords.StopWords( "file://%s" % self.config['stopwords'] )
        # load default filters (TODO put it into import.yaml !!)
        filtertag = ngram.PosTagFilter()
        filterContent = ngram.Filter()
        defaultextractionfilters = [filtertag,filterContent]
        # sends indexer to the file parser
        if index is True:
            index=self.index
        else:
            index = None
        # loads the source file
        dsn = format+"://"+path
        #self.logger.debug(dsn)
        fileReader = Reader( dsn,
            delimiter = self.config['delimiter'],
            quotechar = self.config['quotechar'],
            locale = self.config['locale'],
            fields = self.config['fields']
        )
        # loads or creates corpora
        #corporaObj = self.storage.loadCorpora(corpora_id)
        #if corporaObj is not None:

        corporaObj = corpora.Corpora(corpora_id)

        # instanciate extractor class
        extractor = corpora.Extractor( fileReader, corporaObj, self.storage )
        extractor.walkFile( index, defaultextractionfilters, \
            self.config['ngramMin'], self.config['ngramMax'], \
            self.stopwords, overwrite \
        )
        self.commitAll()
        return extractor.corpora

    def exportCorpora(self, periods, corporaid, synthesispath, \
        whitelist=None, userfilters=None, **kwargs):
        """Public acces to tinasoft.data.ngram.exportCorpora()"""
        exporter = Writer('ngram://'+synthesispath, **kwargs)
        if whitelist is not None:
            whitelist = self.getWhitelist(
                whitelist
            )
        return exporter.exportCorpora( self.storage, periods, corporaid, \
            userfilters, whitelist )

    def getWhitelist(self, filepath, **kwargs):
        """
        import an ngram csv file
        returns a whitelist to be used as input of other methods
        """
        importer = Reader('ngram://'+filepath, **kwargs)
        whitelist = importer.importNGrams()
        return whitelist

    def processCooc(self, whitelist, corporaid, periods, userfilters ):
        """
        Main function importing a whitelist and generating cooccurrences
        """
        # process cooccurrences for each period=corpus
        for id in periods:
            cooc = cooccurrences.MapReduce(self.storage, corpus=id, filter=userfilters, whitelist=whitelist)
            cooc.walkCorpus()
            cooc.writeMatrix(True)
            self.commitAll()
        return True

    def exportGraph(self, path, periods, threshold, whitelist=None, degreemax=None):
        """
        returns a GEXF file path
        the graph is an ngram's 'proximity graph
        for a list of periods and an ngram whitelist
        """
        GEXF = Writer('gexf://').ngramCoocGraph(
            db = self.storage,
            periods = periods,
            threshold = threshold,
            whitelist = whitelist,
            degreemax = degreemax
        )
        #fileid = "%s-%s_"%(threshold[0],threshold[1])
        #path = fileid+path
        open(path, 'wb').write(GEXF)
        return path

    def exportCooc(self, path, periods, whitelist, **kwargs):
        """
        returns a text file path containing the db cooc
        for a list of periods ans an ngrams whitelist
        """
        exporter = Writer('ngram://'+path, **kwargs)
        return exporter.exportCooc( self.storage, periods, whitelist )

    def getCorpora(self, corporaid):
        return self.serialize(self.storage.loadCorpora(corporaid))

    def getCorpus(self, corpusid):
        return self.serialize(self.storage.loadCorpus(corpusid))

    def getDocument(self, documentid):
        return self.serialize(self.storage.loadDocument(documentid))

    def getNGram(self, ngramid):
        return self.serialize(self.storage.loadNGram(ngramid))

    def listCorpora(self, default=None):
        if default is None:
            default=[]
        select = self.storage.select('Corpora')
        try:
            while 1:
                default += [select.next()[1]]
        except StopIteration, si:
            return self.serialize( default )


class ThreadPool:

    """Flexible thread pool class.  Creates a pool of threads, then
    accepts tasks that will be dispatched to the next available
    thread."""

    def __init__(self, numThreads=4):

        """Initialize the thread pool with numThreads workers."""

        self.__threads = []
        self.__resizeLock = threading.Condition(threading.Lock())
        self.__taskLock = threading.Condition(threading.Lock())
        self.__tasks = []
        self.__isJoining = False
        self.setThreadCount(numThreads)

    def setThreadCount(self, newNumThreads):

        """ External method to set the current pool size.  Acquires
        the resizing lock, then calls the internal version to do real
        work."""

        # Can't change the thread count if we're shutting down the pool!
        if self.__isJoining:
            return False

        self.__resizeLock.acquire()
        try:
            self.__setThreadCountNolock(newNumThreads)
        finally:
            self.__resizeLock.release()
        return True

    def __setThreadCountNolock(self, newNumThreads):

        """Set the current pool size, spawning or terminating threads
        if necessary.  Internal use only; assumes the resizing lock is
        held."""

        # If we need to grow the pool, do so
        while newNumThreads > len(self.__threads):
            newThread = ThreadPoolThread(self)
            self.__threads.append(newThread)
            newThread.start()
        # If we need to shrink the pool, do so
        while newNumThreads < len(self.__threads):
            self.__threads[0].goAway()
            del self.__threads[0]

    def getThreadCount(self):

        """Return the number of threads in the pool."""

        self.__resizeLock.acquire()
        try:
            return len(self.__threads)
        finally:
            self.__resizeLock.release()

    def queueTask(self, task, args=(), kwargs={}, taskCallback=None):

        """Insert a task into the queue.  task must be callable;
        args and taskCallback can be None."""

        if self.__isJoining == True:
            return False
        if not callable(task):
            return False

        self.__taskLock.acquire()
        try:
            self.__tasks.append((task, args, kwargs, taskCallback))
            return True
        finally:
            self.__taskLock.release()

    def getNextTask(self):

        """ Retrieve the next task from the task queue.  For use
        only by ThreadPoolThread objects contained in the pool."""

        self.__taskLock.acquire()
        try:
            if self.__tasks == []:
                return (None, None, None, None)
            else:
                return self.__tasks.pop(0)
        finally:
            self.__taskLock.release()

    def joinAll(self, waitForTasks = True, waitForThreads = True):

        """ Clear the task queue and terminate all pooled threads,
        optionally allowing the tasks and threads to finish."""

        # Mark the pool as joining to prevent any more task queueing
        self.__isJoining = True

        # Wait for tasks to finish
        if waitForTasks:
            while self.__tasks != []:
                sleep(0.1)

        # Tell all the threads to quit
        self.__resizeLock.acquire()
        try:
            # Wait until all threads have exited
            if waitForThreads:
                for t in self.__threads:
                    t.goAway()
                for t in self.__threads:
                    t.join()
                    # print t,"joined"
                    del t
            self.__setThreadCountNolock(0)
            self.__isJoining = True

            # Reset the pool for potential reuse
            self.__isJoining = False
        finally:
            self.__resizeLock.release()



class ThreadPoolThread(threading.Thread):

    """ Pooled thread class. """

    threadSleepTime = 0.1

    def __init__(self, pool):

        """ Initialize the thread and remember the pool. """

        threading.Thread.__init__(self)
        self.__pool = pool
        self.__isDying = False

    def run(self):

        """ Until told to quit, retrieve the next task and execute
        it, calling the callback if any.  """

        while self.__isDying == False:
            cmd, args, kwargs, callback = self.__pool.getNextTask()
            # If there's nothing to do, just sleep a bit
            if cmd is None:
                sleep(ThreadPoolThread.threadSleepTime)
            elif callback is None:
                cmd(args, kwargs)
            else:
                #_logger.debug( cmd )
                #_logger.debug( args )
                #_logger.debug( kwargs )
                #_logger.debug( callback )
                callback(cmd(*args, **kwargs))

    def goAway(self):

        """ Exit the run loop next time through."""
        self.__isDying = True
