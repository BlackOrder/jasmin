#!/usr/bin/python3

import os
import signal
import sys
import traceback
import logging
import ntpath

from lockfile import FileLock, LockTimeout, AlreadyLocked
from twisted.internet import reactor, defer
from twisted.python import usage

from jasmin.managers.configs import DLRLookupConfig
from jasmin.managers.dlr import DLRLookup
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.redis.client import ConnectionWithConfiguration
from jasmin.redis.configs import RedisForJasminConfig
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', '%s/etc/jasmin/' % ROOT_PATH)

LOG_CATEGORY = "jasmin-dlrlookup-daemon"

class Options(usage.Options):
    optParameters = [
        ['config', 'c', '%s/dlrlookupd.cfg' % CONFIG_PATH,
         'Jasmin dlrlookupd configuration file'],
        ['id', 'i', 'master',
         'Daemon id, need to be different for each dlrlookupd daemon'],
    ]

    optFlags = [
    ]


class DlrlookupDaemon(BaseDaemon):
    def __init__(self, opt):
        super(DlrlookupDaemon, self).__init__(opt)

        self.log = logging.getLogger(LOG_CATEGORY)
        self.log.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(process)d %(message)s', '%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)
        self.log.propagate = False

    @defer.inlineCallbacks
    def startRedisClient(self):
        """Start Redis Client"""
        RedisForJasminConfigInstance = RedisForJasminConfig(self.options['config'])

        # This is a separate process: do not log to same log_file as Jasmin Daemon
        # Refs #629
        RedisForJasminConfigInstance.log_file = '%s/dlrlookupd-%s' % ntpath.split(RedisForJasminConfigInstance.log_file)

        self.components['rc'] = yield ConnectionWithConfiguration(RedisForJasminConfigInstance)
        # Authenticate and select db
        if RedisForJasminConfigInstance.password is not None:
            yield self.components['rc'].auth(RedisForJasminConfigInstance.password)
            yield self.components['rc'].select(RedisForJasminConfigInstance.dbid)

    def stopRedisClient(self):
        """Stop AMQP Broker"""
        return self.components['rc'].disconnect()

    def startAMQPBrokerService(self):
        """Start AMQP Broker"""

        AMQPServiceConfigInstance = AmqpConfig(self.options['config'])
        
        # This is a separate process: do not log to same log_file as Jasmin Daemon
        AMQPServiceConfigInstance.log_file = '%s/dlrlookupd-%s' % ntpath.split(AMQPServiceConfigInstance.log_file)

        self.components['amqp-broker-factory'] = AmqpFactory(AMQPServiceConfigInstance)
        self.components['amqp-broker-factory'].preConnect()

        # Add service
        self.components['amqp-broker-client'] = reactor.connectTCP(
            AMQPServiceConfigInstance.host,
            AMQPServiceConfigInstance.port,
            self.components['amqp-broker-factory'])

    def stopAMQPBrokerService(self):
        """Stop AMQP Broker"""

        return self.components['amqp-broker-client'].disconnect()

    @defer.inlineCallbacks
    def startDLRLookupService(self):
        """Start DLRLookup"""

        DLRLookupConfigInstance = DLRLookupConfig(self.options['config'])

        # This is a separate process: do not log to same log_file as Jasmin sm-listener
        # Refs #629
        DLRLookupConfigInstance.log_file = '%s/dlrlookupd-%s' % ntpath.split(DLRLookupConfigInstance.log_file)

        self.components['dlrlookup'] = DLRLookup(DLRLookupConfigInstance, self.components['amqp-broker-factory'],
                                                 self.components['rc'])
        yield self.components['dlrlookup'].subscribe()

    @defer.inlineCallbacks
    def startDaemon(self):
        """Start DLRLookup Daemon"""
        self.log.info("Starting DLRLookup Daemon ...")

        ########################################################
        # Connect to redis server
        try:
            yield self.startRedisClient()
        except Exception as e:
            self.log.error("  Cannot start RedisClient: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  RedisClient Started.")

        ########################################################
        # Start AMQP Broker
        try:
            yield self.startAMQPBrokerService()
            yield self.components['amqp-broker-factory'].getChannelReadyDeferred()
        except Exception as e:
            self.log.error("  Cannot start AMQP Broker: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  AMQP Broker Started.")
        
        ########################################################
            
        try:
            self.startDLRLookupService()
        except Exception as e:
            self.log.error("  Cannot start DLRLookup: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  DLRLookup Started.")

        ########################################################
        # register services stop on channelDown
        self.components['amqp-broker-factory'].getChannelDownDeferred().addCallback(self.AMQPDownHandler)


    def AMQPDownHandler(self, ignore):
        """Handle AMQP Broker disconnection"""
        self.log.error("AMQP Broker disconnected, pausing DLRLookupd services ...")
        self.components['amqp-broker-factory'].getChannelReadyDeferred().addCallback(self.refreshServices)


    @defer.inlineCallbacks
    def refreshServices(self, ignore):
        """Refresh DLRLookupd services"""
        self.log.info("Refreshing DLRLookupd services ...")

        self.components['amqp-broker-factory'].getChannelDownDeferred().addCallback(self.AMQPDownHandler)
        try:
            self.startDLRLookupService()
        except Exception as e:
            self.log.error("  Cannot refresh DLRLookup: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  DLRLookup Refreshed.")

    @defer.inlineCallbacks
    def stopDaemons(self):
        """Stop DLRLookupd daemon"""
        self.log.info("Stopping DLRLookupd Daemon ...")

        if 'rc' in self.components:
            yield self.stopRedisClient()
            self.log.info("  RedisClient disconnected.")

        if 'amqp-broker-client' in self.components:
            yield self.stopAMQPBrokerService()
            self.log.info("  AMQP Broker disconnected.")
            
        reactor.stop()

    def sighandler_stop(self, signum, frame):
        """Handle stop signal cleanly"""
        self.log.info("Received signal to stop Jasmin Dlrlookup Daemon")

        return self.stopDaemons()


if __name__ == '__main__':
    lock = None
    try:
        options = Options()
        options.parseOptions()

        # Must not be executed simultaneously (c.f. #265)
        lock = FileLock("/tmp/dlrlookupd-%s" % options['id'])

        # Ensure there are no paralell runs of this script
        lock.acquire(timeout=2)

        # Prepare to start
        ja_d = DlrlookupDaemon(options)
        # Setup signal handlers
        signal.signal(signal.SIGINT, ja_d.sighandler_stop)
        signal.signal(signal.SIGTERM, ja_d.sighandler_stop)
        # Start DlrlookupDaemon
        ja_d.startDaemon()

        reactor.run()
    except usage.UsageError as errortext:
        print('%s: %s' % (sys.argv[0], errortext))
        print('%s: Try --help for usage details.' % (sys.argv[0]))
    except LockTimeout:
        print("Lock not acquired ! exiting")
    except AlreadyLocked:
        print("There's another instance on dlrlookupd running, exiting.")
    finally:
        # Release the lock
        if lock is not None and lock.i_am_locking():
            lock.release()
