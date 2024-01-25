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
from jasmin.routing.configs import deliverSmThrowerConfig
from jasmin.routing.throwers import deliverSmThrower
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon


CONFIG_PATH = os.getenv('CONFIG_PATH', '%s/etc/jasmin/' % ROOT_PATH)

LOG_CATEGORY = "jasmin-deliversm-daemon"

class Options(usage.Options):
    optParameters = [
        ['config', 'c', '%s/deliversm.cfg' % CONFIG_PATH,
         'Jasmin deliversmd configuration file'],
        ['id', 'i', 'master',
         'Daemon id, need to be different for each deliversmd daemon'],
    ]

    optFlags = [
    ]


class DeliverSmDaemon(BaseDaemon):
    def __init__(self, opt):
        super(DeliverSmDaemon, self).__init__(opt)

        self.log = logging.getLogger(LOG_CATEGORY)
        self.log.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(process)d %(message)s', '%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)
        self.log.propagate = False

    @defer.inlineCallbacks
    def startAMQPBrokerService(self):
        """Start AMQP Broker"""

        AMQPServiceConfigInstance = AmqpConfig(self.options['config'])
        
        # This is a separate process: do not log to same log_file as Jasmin Daemon
        AMQPServiceConfigInstance.log_file = '%s/deliversmd-%s' % ntpath.split(AMQPServiceConfigInstance.log_file)

        self.components['amqp-broker-factory'] = AmqpFactory(AMQPServiceConfigInstance)
        self.components['amqp-broker-factory'].preConnect()

        # Add service
        self.components['amqp-broker-client'] = yield reactor.connectTCP(
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

    def startdeliverSmThrowerService(self):
        """Start deliverSmThrower"""

        deliverThrowerConfigInstance = deliverSmThrowerConfig(self.options['config'])
        
        # This is a separate process: do not log to same log_file as Jasmin Daemon
        deliverThrowerConfigInstance.log_file = '%s/deliversmd-%s' % ntpath.split(deliverThrowerConfigInstance.log_file)
        
        self.components['deliversm-thrower'] = deliverSmThrower(deliverThrowerConfigInstance)
        self.components['deliversm-thrower'].addSmpps(self.components['smpps-pb-client'])

        # AMQP Broker is used to listen to deliver_sm queue
        return self.components['deliversm-thrower'].addAmqpBroker(self.components['amqp-broker-factory'])

    @defer.inlineCallbacks
    def stopdeliverSmThrowerService(self):
        """Stop deliverSmThrower"""
        return self.components['deliversm-thrower'].stopService()

    @defer.inlineCallbacks
    def start(self):
        """Start Deliver-Sm Daemon"""
        self.log.info("Starting Deliver-Sm Daemon ...")

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
                # Start deliverSmThrower
                try:
                    yield self.startdeliverSmThrowerService()
                    self.log.info("  deliverSmThrower Started.")

                except Exception as e:
                    self.log.error("  Cannot start deliverSmThrower: %s\n%s" % (e, traceback.format_exc()))
            except Exception as e:
                self.log.error("  Cannot start SMPPServerPBClient: %s\n%s" % (e, traceback.format_exc()))
        except Exception as e:
            self.log.error("  Cannot start AMQP Broker: %s\n%s" % (e, traceback.format_exc()))

    def AMQPDownHandler(self, ignore=None):
        """Pause Deliver-Smd services"""
        self.log.error("AMQP Broker disconnected, pausing Deliver-Smd services ...")
        if self.components['amqp-broker-factory'].connected is True:
            return self.refreshServices()

        if self.components['amqp-broker-factory'].connectionRetry:
            self.components['amqp-broker-factory'].getChannelReadyDeferred().addCallback(self.refreshServices).addErrback(self.stop)
        else:
            self.stop()

    @defer.inlineCallbacks
    def refreshServices(self, ignore=None):
        """Refresh Deliver-Smd services"""
        self.log.info("Refreshing Deliver-Smd services ...")

        self.components['amqp-broker-factory'].getChannelDownDeferred().addCallback(self.AMQPDownHandler)

        ########################################################
        try:
            # Refresh deliverSmThrower
            if 'deliversm-thrower' in self.components:
                yield self.stopdeliverSmThrowerService()
            yield self.startdeliverSmThrowerService()
        except Exception as e:
            self.log.error("  Cannot refresh deliverSmThrower: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  deliverSmThrower Refreshed.")

    @defer.inlineCallbacks
    def stop(self, ignore=None):
        """Stop Deliver-Sm daemon"""
        self.log.info("Stopping Deliver-Sm Daemon ...")

        if 'smpps-pb-client' in self.components:
            yield self.stopSMPPServerPBClient()
            self.log.info("  SMPPServerPBClient Started.")

        if 'deliversm-thrower' in self.components:
            yield self.stopdeliverSmThrowerService()
            self.log.info("  deliverSmThrower stopped.")

        if 'amqp-broker-client' in self.components:
            yield self.stopAMQPBrokerService()
            self.log.info("  AMQP Broker disconnected.")
        
        yield reactor.stop()

    def sighandler_stop(self, signum, frame):
        """Handle stop signal cleanly"""
        self.log.info("Received signal to stop Deliver-Sm Daemon")

        return self.stop()

if __name__ == '__main__':
    lock = None
    try:
        options = Options()
        options.parseOptions()

        # Must not be executed simultaneously (c.f. #265)
        lock = FileLock("/tmp/deliversmd-%s" % options['id'])

        # Ensure there are no paralell runs of this script
        lock.acquire(timeout=2)

        # Prepare to start
        deliversm_d = DeliverSmDaemon(options)
        # Setup signal handlers
        signal.signal(signal.SIGINT, deliversm_d.sighandler_stop)
        signal.signal(signal.SIGTERM, deliversm_d.sighandler_stop)
        # Start DeliverSmDaemon
        deliversm_d.start()

        reactor.run()
    except usage.UsageError as errortext:
        print('%s: %s' % (sys.argv[0], errortext))
        print('%s: Try --help for usage details.' % (sys.argv[0]))
    except LockTimeout:
        print("Lock not acquired ! exiting")
    except AlreadyLocked:
        print("There's another instance on deliversmd running, exiting.")
    finally:
        # Release the lock
        if lock is not None and lock.i_am_locking():
            lock.release()
