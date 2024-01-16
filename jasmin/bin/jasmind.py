#!/usr/bin/python3

import os
import signal
import sys
import traceback
import logging
import ntpath

from lockfile import FileLock, LockTimeout, AlreadyLocked
from twisted.cred import portal
from twisted.cred.checkers import AllowAnonymousAccess, InMemoryUsernamePasswordDatabaseDontUse
from twisted.internet import reactor, defer
from twisted.python import usage
from twisted.spread import pb
from twisted.web import server

from jasmin.interceptor.configs import InterceptorPBClientConfig
from jasmin.interceptor.proxies import InterceptorPBProxy
from jasmin.managers.clients import SMPPClientManagerPB
from jasmin.managers.configs import SMPPClientPBConfig, DLRLookupConfig
from jasmin.managers.dlr import DLRLookup
from jasmin.protocols.cli.configs import JCliConfig
from jasmin.protocols.cli.factory import JCliFactory
from jasmin.protocols.http.configs import HTTPApiConfig
from jasmin.protocols.http.server import HTTPApi
from jasmin.protocols.smpp.configs import SMPPServerConfig, SMPPServerPBConfig
from jasmin.protocols.smpp.factory import SMPPServerFactory
from jasmin.protocols.smpp.pb import SMPPServerPB
from jasmin.queues.configs import AmqpConfig
from jasmin.queues.factory import AmqpFactory
from jasmin.redis.client import ConnectionWithConfiguration
from jasmin.redis.configs import RedisForJasminConfig
from jasmin.routing.configs import RouterPBConfig, deliverSmThrowerConfig, DLRThrowerConfig
from jasmin.routing.router import RouterPB
from jasmin.routing.throwers import deliverSmThrower, DLRThrower
from jasmin.tools.cred.checkers import RouterAuthChecker
from jasmin.tools.cred.portal import JasminPBRealm
from jasmin.tools.cred.portal import SmppsRealm
from jasmin.tools.spread.pb import JasminPBPortalRoot
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', '%s/etc/jasmin/' % ROOT_PATH)

LOG_CATEGORY = "jasmin-daemon"


class Options(usage.Options):
    optParameters = [
        ['config', 'c', '%s/jasmin.cfg' % CONFIG_PATH,
         'Jasmin configuration file'],
        ['username', 'u', None,
         'jCli username used to load configuration profile on startup'],
        ['password', 'p', None,
         'jCli password used to load configuration profile on startup'],
    ]

    optFlags = [
        ['disable-smpp-server', None, 'Do not start SMPP Server service'],
        ['enable-dlr-thrower', None, 'Enable DLR Thrower service (not recommended: start the dlrd daemon instead)'],
        ['enable-dlr-lookup', None, 'Enable DLR Lookup service (not recommended: start the dlrlookupd daemon instead)'],
        ['enable-deliver-thrower', None, 'Enable DeliverSm Thrower service (not recommended: start the deliversmd daemon instead)'],
        ['disable-http-api', None, 'Do not start HTTP API'],
        ['disable-jcli', None, 'Do not start jCli console'],
        ['enable-interceptor-client', None, 'Start Interceptor client'],
    ]


class JasminDaemon(BaseDaemon):
    def __init__(self, opt):
        super(JasminDaemon, self).__init__(opt)

        self.log = logging.getLogger(LOG_CATEGORY)
        self.log.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(process)d %(message)s', '%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)
        self.log.propagate = False

    @defer.inlineCallbacks
    def startRedisClient(self):
        """Start Redis client"""
        RedisForJasminConfigInstance = RedisForJasminConfig(self.options['config'])
        self.components['rc'] = yield ConnectionWithConfiguration(RedisForJasminConfigInstance)
        # Authenticate and select db
        if RedisForJasminConfigInstance.password is not None:
            yield self.components['rc'].auth(RedisForJasminConfigInstance.password)
            yield self.components['rc'].select(RedisForJasminConfigInstance.dbid)

    @defer.inlineCallbacks
    def stopRedisClient(self):
        """Stop Redis client"""
        yield self.components['rc'].disconnect()

    def startAMQPBrokerService(self):
        """Start AMQP Broker"""

        AMQPServiceConfigInstance = AmqpConfig(self.options['config'])
        amqp_factory = AmqpFactory(AMQPServiceConfigInstance)
        amqp_factory.preConnect()
        
        return {
            'factory': amqp_factory,
            'client': reactor.connectTCP(
                    AMQPServiceConfigInstance.host,
                    AMQPServiceConfigInstance.port,
                    amqp_factory
                )
        }

    @defer.inlineCallbacks
    def startRouterPBService(self):
        """Start Router PB server"""

        RouterPBConfigInstance = RouterPBConfig(self.options['config'])
        
        self.components['router-pb-factory'] = RouterPB(RouterPBConfigInstance)

        # Set authentication portal
        p = portal.Portal(JasminPBRealm(self.components['router-pb-factory']))
        if RouterPBConfigInstance.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(RouterPBConfigInstance.admin_username,
                      RouterPBConfigInstance.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())
        jPBPortalRoot = JasminPBPortalRoot(p)

        # Add service
        self.components['router-pb-server'] = reactor.listenTCP(
            RouterPBConfigInstance.port,
            pb.PBServerFactory(jPBPortalRoot),
            interface=RouterPBConfigInstance.bind)

        # AMQP Broker is used to listen to deliver_sm/dlr queues
        self.components['router-pb-amqp'] = self.startAMQPBrokerService()
        yield self.components['router-pb-amqp']['factory'].getChannelReadyDeferred()
        self.log.info("RouterPB AMQP service started.")
        self.components['router-pb-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshRouterPBAMQPService)

        yield self.components['router-pb-factory'].addAmqpBroker(self.components['router-pb-amqp']['factory'])

    @defer.inlineCallbacks
    def refreshRouterPBAMQPService(self, ignore=None):
        """Refresh AMQP Broker connection"""
        self.log.info("Refreshing RouterPB AMQP service ...")
        if self.components['router-pb-amqp']['factory'].connected is False:
            try:
                if self.components['router-pb-amqp']['factory'].connectionRetry:
                    yield self.components['router-pb-amqp']['factory'].getChannelReadyDeferred()
                else:
                    raise Exception('Connection retry is disabled')
            except Exception as e:
                self.log.error("  Cannot connect to AMQP Broker: %s\n%s" % (e, traceback.format_exc()))
                self.stop()
                return
        self.components['router-pb-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshRouterPBAMQPService)
        yield self.components['router-pb-factory'].addAmqpBroker(self.components['router-pb-amqp']['factory'])

    @defer.inlineCallbacks
    def stopRouterPBService(self):
        """Stop Router PB server"""
        yield self.components['router-pb-server'].stopListening()
        yield self.components['router-pb-amqp']['client'].disconnect()

    @defer.inlineCallbacks
    def startSMPPClientManagerPBService(self):
        """Start SMPP Client Manager PB server"""

        SMPPClientPBConfigInstance = SMPPClientPBConfig(self.options['config'])
        self.components['smppcm-pb-factory'] = SMPPClientManagerPB(SMPPClientPBConfigInstance)

        # Set authentication portal
        p = portal.Portal(JasminPBRealm(self.components['smppcm-pb-factory']))
        if SMPPClientPBConfigInstance.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(SMPPClientPBConfigInstance.admin_username, SMPPClientPBConfigInstance.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())
        jPBPortalRoot = JasminPBPortalRoot(p)

        # Add service
        self.components['smppcm-pb-server'] = reactor.listenTCP(
            SMPPClientPBConfigInstance.port,
            pb.PBServerFactory(jPBPortalRoot),
            interface=SMPPClientPBConfigInstance.bind)

        # AMQP Broker is used to listen to submit_sm queues and publish to deliver_sm/dlr queues
        self.components['smppcm-pb-amqp'] = self.startAMQPBrokerService()
        yield self.components['smppcm-pb-amqp']['factory'].getChannelReadyDeferred()
        self.components['smppcm-pb-amqp']['factory'].getExitDeferred().addCallback(self.stop)
        self.log.info("SMPPClientManagerPB AMQP service started.")

        self.components['smppcm-pb-factory'].addAmqpBroker(self.components['smppcm-pb-amqp']['factory'])
        self.components['smppcm-pb-factory'].addRedisClient(self.components['rc'])
        self.components['smppcm-pb-factory'].addRouterPB(self.components['router-pb-factory'])

        # Add interceptor if enabled:
        if 'interceptor-pb-client' in self.components:
            self.components['smppcm-pb-factory'].addInterceptorPBClient(
                self.components['interceptor-pb-client'])

    @defer.inlineCallbacks
    def stopSMPPClientManagerPBService(self):
        """Stop SMPP Client Manager PB server"""
        yield self.components['smppcm-pb-server'].stopListening()
        yield self.components['smppcm-pb-amqp']['client'].disconnect()

    @defer.inlineCallbacks
    def startDLRLookupService(self):
        """Start DLRLookup"""
        self.components['dlrlookup-amqp'] = self.startAMQPBrokerService()
        yield self.components['dlrlookup-amqp']['factory'].getChannelReadyDeferred()
        self.log.info("DLRLookup AMQP service started.")
        self.components['dlrlookup-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshDLRLookupAMQPService)

        DLRLookupConfigInstance = DLRLookupConfig(self.options['config'])
        self.components['dlrlookup'] = DLRLookup(DLRLookupConfigInstance, self.components['dlrlookup-amqp']['factory'],
                                                 self.components['rc'])
        yield self.components['dlrlookup'].subscribe()

    @defer.inlineCallbacks
    def refreshDLRLookupAMQPService(self, ignore=None):
        """Refresh AMQP Broker connection"""
        self.log.info("Refreshing DLRLookup AMQP service ...")
        if self.components['dlrlookup-amqp']['factory'].connected is False:
            try:
                if self.components['dlrlookup-amqp']['factory'].connectionRetry:
                    yield self.components['dlrlookup-amqp']['factory'].getChannelReadyDeferred()
                else:
                    raise Exception('Connection retry is disabled')
            except Exception as e:
                self.log.error("  Cannot connect to AMQP Broker: %s\n%s" % (e, traceback.format_exc()))
                self.stop()
                return
        self.components['dlrlookup-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshDLRLookupAMQPService)
        yield self.components['dlrlookup'].subscribe()

    @defer.inlineCallbacks
    def stopDLRLookupService(self):
        """Stop DLRLookup"""
        yield self.components['dlrlookup-amqp']['client'].disconnect()

    def startSMPPServerPBService(self):
        """Start SMPP Server PB server"""

        SMPPServerPBConfigInstance = SMPPServerPBConfig(self.options['config'])
        self.components['smpps-pb-factory'] = SMPPServerPB(SMPPServerPBConfigInstance)

        # Set authentication portal
        p = portal.Portal(JasminPBRealm(self.components['smpps-pb-factory']))
        if SMPPServerPBConfigInstance.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(SMPPServerPBConfigInstance.admin_username, SMPPServerPBConfigInstance.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())
        jPBPortalRoot = JasminPBPortalRoot(p)

        # Add service
        self.components['smpps-pb-server'] = reactor.listenTCP(
            SMPPServerPBConfigInstance.port,
            pb.PBServerFactory(jPBPortalRoot),
            interface=SMPPServerPBConfigInstance.bind)

    def stopSMPPServerPBService(self):
        """Stop SMPP Server PB"""
        return self.components['smpps-pb-server'].stopListening()

    def startSMPPServerService(self):
        """Start SMPP Server"""

        SMPPServerConfigInstance = SMPPServerConfig(self.options['config'])

        # Set authentication portal
        p = portal.Portal(
            SmppsRealm(
                SMPPServerConfigInstance.id,
                self.components['router-pb-factory']))
        p.registerChecker(RouterAuthChecker(self.components['router-pb-factory']))

        # SMPPServerFactory init
        self.components['smpp-server-factory'] = SMPPServerFactory(
            SMPPServerConfigInstance,
            auth_portal=p,
            RouterPB=self.components['router-pb-factory'],
            SMPPClientManagerPB=self.components['smppcm-pb-factory'])

        # Start server
        self.components['smpp-server'] = reactor.listenTCP(
            SMPPServerConfigInstance.port,
            self.components['smpp-server-factory'],
            interface=SMPPServerConfigInstance.bind)

        # Add interceptor if enabled:
        if 'interceptor-pb-client' in self.components:
            self.components['smpp-server-factory'].addInterceptorPBClient(
                self.components['interceptor-pb-client'])

    def stopSMPPServerService(self):
        """Stop SMPP Server"""
        return self.components['smpp-server'].stopListening()

    @defer.inlineCallbacks
    def startdeliverSmThrowerService(self):
        """Start deliverSmThrower"""

        deliverThrowerConfigInstance = deliverSmThrowerConfig(self.options['config'])
        self.components['deliversm-thrower'] = deliverSmThrower(deliverThrowerConfigInstance)
        self.components['deliversm-thrower'].addSmpps(self.components['smpp-server-factory'])

        # AMQP Broker is used to listen to deliver_sm queue
        self.components['deliversm-thrower-amqp'] = self.startAMQPBrokerService()
        yield self.components['deliversm-thrower-amqp']['factory'].getChannelReadyDeferred()
        self.log.info("deliverSmThrower AMQP service started.")
        self.components['deliversm-thrower-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshdeliverSmThrowerAMQPService)

        yield self.components['deliversm-thrower'].addAmqpBroker(self.components['deliversm-thrower-amqp']['factory'])

    @defer.inlineCallbacks
    def refreshdeliverSmThrowerAMQPService(self, ignore=None):
        """Refresh AMQP Broker connection"""
        self.log.info("Refreshing deliverSmThrower AMQP service ...")
        if self.components['deliversm-thrower-amqp']['factory'].connected is False:
            try:
                if self.components['deliversm-thrower-amqp']['factory'].connectionRetry:
                    yield self.components['deliversm-thrower-amqp']['factory'].getChannelReadyDeferred()
                else:
                    raise Exception('Connection retry is disabled')
            except Exception as e:
                self.log.error("  Cannot connect to AMQP Broker: %s\n%s" % (e, traceback.format_exc()))
                self.stop()
                return
        self.components['deliversm-thrower-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshdeliverSmThrowerAMQPService)
        yield self.components['deliversm-thrower'].addAmqpBroker(self.components['deliversm-thrower-amqp']['factory'])

    @defer.inlineCallbacks
    def stopdeliverSmThrowerService(self):
        """Stop deliverSmThrower"""
        yield self.components['deliversm-thrower'].stopService()
        yield self.components['deliversm-thrower-amqp']['client'].disconnect()

    @defer.inlineCallbacks
    def startDLRThrowerService(self):
        """Start DLRThrower"""

        DLRThrowerConfigInstance = DLRThrowerConfig(self.options['config'])
        self.components['dlr-thrower'] = DLRThrower(DLRThrowerConfigInstance)
        self.components['dlr-thrower'].addSmpps(self.components['smpp-server-factory'])

        # AMQP Broker is used to listen to DLRThrower queue
        self.components['dlr-thrower-amqp'] = self.startAMQPBrokerService()
        yield self.components['dlr-thrower-amqp']['factory'].getChannelReadyDeferred()
        self.log.info("DLRThrower AMQP service started.")
        self.components['dlr-thrower-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshDLRThrowerAMQPService)

        yield self.components['dlr-thrower'].addAmqpBroker(self.components['dlr-thrower-amqp']['factory'])

    @defer.inlineCallbacks
    def refreshDLRThrowerAMQPService(self, ignore=None):
        """Refresh AMQP Broker connection"""
        self.log.info("Refreshing DLRThrower AMQP service ...")
        if self.components['dlr-thrower-amqp']['factory'].connected is False:
            try:
                if self.components['dlr-thrower-amqp']['factory'].connectionRetry:
                    yield self.components['dlr-thrower-amqp']['factory'].getChannelReadyDeferred()
                else:
                    raise Exception('Connection retry is disabled')
            except Exception as e:
                self.log.error("  Cannot connect to AMQP Broker: %s\n%s" % (e, traceback.format_exc()))
                self.stop()
                return
        self.components['dlr-thrower-amqp']['factory'].getChannelDownDeferred().addCallback(self.refreshDLRThrowerAMQPService)
        yield self.components['dlr-thrower'].addAmqpBroker(self.components['dlr-thrower-amqp']['factory'])

    @defer.inlineCallbacks
    def stopDLRThrowerService(self):
        """Stop DLRThrower"""
        yield self.components['dlr-thrower'].stopService()
        yield self.components['dlr-thrower-amqp']['client'].disconnect()

    def startHTTPApiService(self):
        """Start HTTP Api"""

        httpApiConfigInstance = HTTPApiConfig(self.options['config'])

        # Add interceptor if enabled:
        if 'interceptor-pb-client' in self.components:
            interceptorpb_client = self.components['interceptor-pb-client']
        else:
            interceptorpb_client = None

        self.components['http-api-factory'] = HTTPApi(
            self.components['router-pb-factory'],
            self.components['smppcm-pb-factory'],
            httpApiConfigInstance,
            interceptorpb_client)

        self.components['http-api-server'] = reactor.listenTCP(
            httpApiConfigInstance.port,
            server.Site(self.components['http-api-factory'], logPath=httpApiConfigInstance.access_log),
            interface=httpApiConfigInstance.bind)

    def stopHTTPApiService(self):
        """Stop HTTP Api"""
        return self.components['http-api-server'].stopListening()

    def startJCliService(self):
        """Start jCli console server"""
        loadConfigProfileWithCreds = {
            'username': self.options['username'],
            'password': self.options['password']}
        JCliConfigInstance = JCliConfig(self.options['config'])
        JCli_f = JCliFactory(
            JCliConfigInstance,
            self.components['smppcm-pb-factory'],
            self.components['router-pb-factory'],
            self.components['smpp-server-factory'],
            loadConfigProfileWithCreds)

        self.components['jcli-server'] = reactor.listenTCP(
            JCliConfigInstance.port,
            JCli_f,
            interface=JCliConfigInstance.bind)

    def stopJCliService(self):
        """Stop jCli console server"""
        return self.components['jcli-server'].stopListening()

    def startInterceptorPBClient(self):
        """Start Interceptor client"""

        InterceptorPBClientConfigInstance = InterceptorPBClientConfig(self.options['config'])
        self.components['interceptor-pb-client'] = InterceptorPBProxy()

        return self.components['interceptor-pb-client'].connect(
            InterceptorPBClientConfigInstance.host,
            InterceptorPBClientConfigInstance.port,
            InterceptorPBClientConfigInstance.username,
            InterceptorPBClientConfigInstance.password,
            retry=True)

    def stopInterceptorPBClient(self):
        """Stop Interceptor client"""

        if self.components['interceptor-pb-client'].isConnected:
            return self.components['interceptor-pb-client'].disconnect()

    @defer.inlineCallbacks
    def start(self):
        """Start Jasmind daemon"""
        self.log.info("Starting Jasmin Daemon ...")

        # Requirements check begin:
        ########################################################
        if self.options['enable-interceptor-client']:
            try:
                # [optional] Start Interceptor client
                yield self.startInterceptorPBClient()
            except Exception as e:
                self.log.error("  Cannot connect to interceptor: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  Interceptor client Started.")
        # Requirements check end.

        ########################################################
        # Connect to redis server
        try:
            yield self.startRedisClient()
        except Exception as e:
            self.log.error("  Cannot start RedisClient: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  RedisClient Started.")

        ########################################################
        # Start Router PB server
        try:
            yield self.startRouterPBService()
        except Exception as e:
            self.log.error("  Cannot start RouterPB: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  RouterPB Started.")

        ########################################################
        # Start SMPP Client connector manager and add rc
        try:
            yield self.startSMPPClientManagerPBService()
        except Exception as e:
            self.log.error("  Cannot start SMPPClientManagerPB: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  SMPPClientManagerPB Started.")

        ########################################################
        if self.options['enable-dlr-lookup']:
            try:
                # [optional] Start DLR Lookup
                yield self.startDLRLookupService()
            except Exception as e:
                self.log.error("  Cannot start DLRLookup: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  DLRLookup Started.")

        ########################################################
        if not self.options['disable-smpp-server']:
            try:
                # [optional] Start SMPP Server
                self.startSMPPServerService()
            except Exception as e:
                self.log.error("  Cannot start SMPPServer: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  SMPPServer Started.")

            try:
                # [optional] Start SMPP Server PB
                self.startSMPPServerPBService()
                self.components['smpps-pb-factory'].addSmpps(self.components['smpp-server-factory'])
            except Exception as e:
                self.log.error("  Cannot start SMPPServerPB: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  SMPPServer Started.")

        ########################################################
        if self.options['enable-deliver-thrower']:
            try:
                # [optional] Start deliverSmThrower
                yield self.startdeliverSmThrowerService()
            except Exception as e:
                self.log.error("  Cannot start deliverSmThrower: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  deliverSmThrower Started.")

        ########################################################
        if self.options['enable-dlr-thrower']:
            try:
                # [optional] Start DLRThrower
                yield self.startDLRThrowerService()
            except Exception as e:
                self.log.error("  Cannot start DLRThrower: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  DLRThrower Started.")

        ########################################################
        if not self.options['disable-http-api']:
            try:
                # [optional] Start HTTP Api
                self.startHTTPApiService()
            except Exception as e:
                self.log.error("  Cannot start HTTPApi: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  HTTPApi Started.")

        ########################################################
        if not self.options['disable-jcli']:
            try:
                # [optional] Start JCli server
                self.startJCliService()
            except Exception as e:
                self.log.error("  Cannot start jCli: %s\n%s" % (e, traceback.format_exc()))
            else:
                self.log.info("  jCli Started.")

    @defer.inlineCallbacks
    def stop(self, ignore=None):
        """Stop Jasmind daemon"""
        self.log.info("Stopping Jasmin Daemon ...")

        if 'jcli-server' in self.components:
            yield self.stopJCliService()
            self.log.info("  jCli stopped.")

        if 'http-api-server' in self.components:
            yield self.stopHTTPApiService()
            self.log.info("  HTTPApi stopped.")

        if 'dlr-thrower' in self.components:
            yield self.stopDLRThrowerService()
            self.log.info("  DLRThrower stopped.")

        if 'deliversm-thrower' in self.components:
            yield self.stopdeliverSmThrowerService()
            self.log.info("  deliverSmThrower stopped.")

        if 'smpps-pb-server' in self.components:
            yield self.stopSMPPServerPBService()
            self.log.info("  SMPPServerPB stopped.")

        if 'smpp-server' in self.components:
            yield self.stopSMPPServerService()
            self.log.info("  SMPPServer stopped.")
        
        if 'dlrlookup' in self.components:
            yield self.stopDLRLookupService()
            self.log.info("  DLRLookup stopped.")

        if 'smppcm-pb-server' in self.components:
            yield self.stopSMPPClientManagerPBService()
            self.log.info("  SMPPClientManagerPB stopped.")

        if 'router-pb-server' in self.components:
            yield self.stopRouterPBService()
            self.log.info("  RouterPB stopped.")

        if 'rc' in self.components:
            yield self.stopRedisClient()
            self.log.info("  RedisClient stopped.")

        # Shutdown requirements:
        if 'interceptor-pb-client' in self.components:
            yield self.stopInterceptorPBClient()
            self.log.info("  Interceptor client stopped.")

        reactor.stop()

    def sighandler_stop(self, signum, frame):
        """Handle stop signal cleanly"""
        self.log.info("Received signal to stop Jasmin Daemon")

        return self.stop()


if __name__ == '__main__':
    lock = None
    try:
        options = Options()
        options.parseOptions()

        # Must not be executed simultaneously (c.f. #265)
        lock = FileLock("/tmp/jasmind")

        # Ensure there are no paralell runs of this script
        lock.acquire(timeout=2)

        # Prepare to start
        ja_d = JasminDaemon(options)
        # Setup signal handlers
        signal.signal(signal.SIGINT, ja_d.sighandler_stop)
        signal.signal(signal.SIGTERM, ja_d.sighandler_stop)
        # Start JasminDaemon
        ja_d.start()

        reactor.run()
    except usage.UsageError as errortext:
        print('%s: %s' % (sys.argv[0], errortext))
        print('%s: Try --help for usage details.' % (sys.argv[0]))
    except LockTimeout:
        print("Lock not acquired ! exiting")
    except AlreadyLocked:
        print("There's another instance on jasmind running, exiting.")
    finally:
        # Release the lock
        if lock is not None and lock.i_am_locking():
            lock.release()
