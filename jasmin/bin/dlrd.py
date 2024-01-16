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

from jasmin.protocols.smpp.configs import SMPPServerPBClientConfig
from jasmin.protocols.smpp.proxies import SMPPServerPBProxy
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.routing.configs import DLRThrowerConfig
from jasmin.routing.throwers import DLRThrower
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', '%s/etc/jasmin/' % ROOT_PATH)

LOG_CATEGORY = "Dlr-daemon"


class Options(usage.Options):
    optParameters = [
        ['config', 'c', '%s/dlr.cfg' % CONFIG_PATH,
         'Jasmin dlrd configuration file'],
        ['id', 'i', 'master',
         'Daemon id, need to be different for each dlrd daemon'],
    ]

    optFlags = [
    ]


class DlrDaemon(BaseDaemon):
    def __init__(self, opt):
        super(DlrDaemon, self).__init__(opt)

        self.log = logging.getLogger(LOG_CATEGORY)
        self.log.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(process)d %(message)s', '%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)
        self.log.propagate = False

    def startAMQPBrokerService(self):
        """Start AMQP Broker"""

        AMQPServiceConfigInstance = AmqpConfig(self.options['config'])
        
        # This is a separate process: do not log to same log_file as Jasmin Daemon
        AMQPServiceConfigInstance.log_file = '%s/dlrd-%s' % ntpath.split(AMQPServiceConfigInstance.log_file)

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

    def startSMPPServerPBClient(self):
        """Start SMPPServerPB client"""

        SMPPServerPBClientConfigInstance = SMPPServerPBClientConfig(self.options['config'])
        self.components['smpps-pb-client'] = SMPPServerPBProxy()

        return self.components['smpps-pb-client'].connect(
            SMPPServerPBClientConfigInstance.host,
            SMPPServerPBClientConfigInstance.port,
            SMPPServerPBClientConfigInstance.username,
            SMPPServerPBClientConfigInstance.password,
            retry=True)

    @defer.inlineCallbacks
    def stopSMPPServerPBClient(self):
        """Stop SMPPServerPB client"""

        if self.components['smpps-pb-client'].isConnected:
            yield self.components['smpps-pb-client'].disconnect()

    def startDLRThrowerService(self):
        """Start DLRThrower"""

        DLRThrowerConfigInstance = DLRThrowerConfig(self.options['config'])
        
        # This is a separate process: do not log to same log_file as Jasmin sm-listener
        DLRThrowerConfigInstance.log_file = '%s/dlrd-%s' % ntpath.split(DLRThrowerConfigInstance.log_file)

        self.components['dlr-thrower'] = DLRThrower(DLRThrowerConfigInstance)
        self.components['dlr-thrower'].addSmpps(self.components['smpps-pb-client'])

        # AMQP Broker is used to listen to DLRThrower queue
        return self.components['dlr-thrower'].addAmqpBroker(self.components['amqp-broker-factory'])

    def stopDLRThrowerService(self):
        """Stop DLRThrower"""
        return self.components['dlr-thrower'].stopService()

    @defer.inlineCallbacks
    def start(self):
        """Start Dlr Daemon"""
        self.log.info("Starting Dlr Daemon ...")

        ########################################################
        # Start AMQP Broker
        try:
            yield self.startAMQPBrokerService()
            yield self.components['amqp-broker-factory'].getChannelReadyDeferred()
            self.log.info("  AMQP Broker Started.")

            ########################################################
            # register services stop on channelDown
            self.components['amqp-broker-factory'].getChannelDownDeferred().addCallback(self.AMQPDownHandler)

            ########################################################
            # Start SMPPServerPB Client
            try:
                yield self.startSMPPServerPBClient()
                self.log.info("  SMPPServerPBClient Started.")

                ########################################################
                # Start DLRThrower
                try:
                    yield self.startDLRThrowerService()
                    self.log.info("  DLRThrower Started.")

                except Exception as e:
                    self.log.error("  Cannot start DLRThrower: %s\n%s" % (e, traceback.format_exc()))
            except Exception as e:
                self.log.error("  Cannot start SMPPServerPBClient: %s\n%s" % (e, traceback.format_exc()))
        except Exception as e:
            self.log.error("  Cannot start AMQP Broker: %s\n%s" % (e, traceback.format_exc()))

    def AMQPDownHandler(self, ignore=None):
        """Handle AMQP Broker disconnection"""
        self.log.error("AMQP Broker disconnected, pausing Dlrd services ...")
        if self.components['amqp-broker-factory'].connected is True:
            return self.refreshServices()

        if self.components['amqp-broker-factory'].connectionRetry:
            self.components['amqp-broker-factory'].getChannelReadyDeferred().addCallback(self.refreshServices).addErrback(self.stop)
        else:
            self.stop()

    @defer.inlineCallbacks
    def refreshServices(self, ignore=None):
        """Refresh Dlrd services"""
        self.log.info("Unpausing Dlrd services ...")

        self.components['amqp-broker-factory'].getChannelDownDeferred().addCallback(self.AMQPDownHandler)

        ########################################################
        try:
            # refresh DLRThrower
            if 'dlr-thrower' in self.components:
                yield self.stopDLRThrowerService()
            yield self.startDLRThrowerService()
        except Exception as e:
            self.log.error("  Cannot refresh DLRThrower: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  DLRThrower Refreshed.")

    @defer.inlineCallbacks
    def stop(self, ignore=None):
        """Stop Dlr daemon"""
        self.log.info("Stopping Dlr Daemon ...")

        if 'smpps-pb-client' in self.components:
            yield self.stopSMPPServerPBClient()
            self.log.info("  SMPPServerPBClient Started.")

        if 'dlr-thrower' in self.components:
            yield self.stopDLRThrowerService()
            self.log.info("  DLRThrower stopped.")

        if 'amqp-broker-client' in self.components:
            yield self.stopAMQPBrokerService()
            self.log.info("  AMQP Broker disconnected.")
        
        reactor.stop()

    def sighandler_stop(self, signum, frame):
        """Handle stop signal cleanly"""
        self.log.info("Received signal to stop Dlr Daemon")

        return self.stop()

if __name__ == '__main__':
    lock = None
    try:
        options = Options()
        options.parseOptions()

        # Must not be executed simultaneously (c.f. #265)
        lock = FileLock("/tmp/dlrd-%s" % options['id'])

        # Ensure there are no paralell runs of this script
        lock.acquire(timeout=2)

        # Prepare to start
        dlr_d = DlrDaemon(options)
        # Setup signal handlers
        signal.signal(signal.SIGINT, dlr_d.sighandler_stop)
        signal.signal(signal.SIGTERM, dlr_d.sighandler_stop)
        # Start DlrDaemon
        dlr_d.start()

        reactor.run()
    except usage.UsageError as errortext:
        print('%s: %s' % (sys.argv[0], errortext))
        print('%s: Try --help for usage details.' % (sys.argv[0]))
    except LockTimeout:
        print("Lock not acquired ! exiting")
    except AlreadyLocked:
        print("There's another instance on dlrd running, exiting.")
    finally:
        # Release the lock
        if lock is not None and lock.i_am_locking():
            lock.release()
