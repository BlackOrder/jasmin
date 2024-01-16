#!/usr/bin/python3

import os
import signal
import sys
import traceback
import logging

from lockfile import FileLock, LockTimeout, AlreadyLocked
from twisted.cred import portal
from twisted.cred.checkers import AllowAnonymousAccess, InMemoryUsernamePasswordDatabaseDontUse
from twisted.internet import reactor, defer
from twisted.python import usage
from twisted.spread import pb

from jasmin.interceptor.configs import InterceptorPBConfig
from jasmin.interceptor.interceptor import InterceptorPB
from jasmin.tools.cred.portal import JasminPBRealm
from jasmin.tools.spread.pb import JasminPBPortalRoot
from jasmin.config import ROOT_PATH
from jasmin.bin import BaseDaemon

CONFIG_PATH = os.getenv('CONFIG_PATH', '%s/etc/jasmin/' % ROOT_PATH)

LOG_CATEGORY = "jasmin-interceptor-daemon"

class Options(usage.Options):
    optParameters = [
        ['config', 'c', '%s/interceptor.cfg' % CONFIG_PATH,
         'Jasmin interceptor configuration file'],
    ]


class InterceptorDaemon(BaseDaemon):
    def __init__(self, opt):
        super(InterceptorDaemon, self).__init__(opt)

        self.log = logging.getLogger(LOG_CATEGORY)
        self.log.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(process)d %(message)s', '%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)
        self.log.propagate = False

    @defer.inlineCallbacks
    def startInterceptorPBService(self):
        """Start Interceptor PB server"""

        InterceptorPBConfigInstance = InterceptorPBConfig(self.options['config'])
        self.components['interceptor-pb-factory'] = InterceptorPB(InterceptorPBConfigInstance)

        # Set authentication portal
        p = portal.Portal(JasminPBRealm(self.components['interceptor-pb-factory']))
        if InterceptorPBConfigInstance.authentication:
            c = InMemoryUsernamePasswordDatabaseDontUse()
            c.addUser(InterceptorPBConfigInstance.admin_username,
                      InterceptorPBConfigInstance.admin_password)
            p.registerChecker(c)
        else:
            p.registerChecker(AllowAnonymousAccess())
        jPBPortalRoot = JasminPBPortalRoot(p)

        # Add service
        self.components['interceptor-pb-server'] = yield reactor.listenTCP(
            InterceptorPBConfigInstance.port,
            pb.PBServerFactory(jPBPortalRoot),
            interface=InterceptorPBConfigInstance.bind)

    def stopInterceptorPBService(self):
        """Stop Interceptor PB server"""
        return self.components['interceptor-pb-server'].stopListening()

    @defer.inlineCallbacks
    def start(self):
        """Start Interceptord daemon"""
        self.log.info("Starting InterceptorPB Daemon ...")

        ########################################################
        # Start Interceptor PB server
        try:
            yield self.startInterceptorPBService()
        except Exception as e:
            self.log.error("  Cannot start Interceptor: %s\n%s" % (e, traceback.format_exc()))
        else:
            self.log.info("  Interceptor Started.")

    @defer.inlineCallbacks
    def stop(self):
        """Stop Interceptord daemon"""
        self.log.info("Stopping Interceptor Daemon ...")

        if 'interceptor-pb-server' in self.components:
            yield self.stopInterceptorPBService()
            self.log.info("  InterceptorPB stopped.")

        reactor.stop()

    def sighandler_stop(self, signum, frame):
        """Handle stop signal cleanly"""
        self.log.info("Received signal to stop Interceptor Daemon")

        return self.stop()


if __name__ == '__main__':
    # Must not be executed simultaneously (c.f. #265)
    lock = FileLock("/tmp/interceptord")

    try:
        options = Options()
        options.parseOptions()

        # Ensure there are no paralell runs of this script
        lock.acquire(timeout=2)

        # Prepare to start
        in_d = InterceptorDaemon(options)
        # Setup signal handlers
        signal.signal(signal.SIGINT, in_d.sighandler_stop)
        signal.signal(signal.SIGTERM, in_d.sighandler_stop)
        # Start InterceptorDaemon
        in_d.start()

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
        if lock.i_am_locking():
            lock.release()
