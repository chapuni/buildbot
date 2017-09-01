# This file is part of Buildbot.  Buildbot is free software: you can
# redistribute it and/or modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation, version 2.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc., 51
# Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Copyright Buildbot Team Members

from __future__ import absolute_import
from __future__ import print_function
from future.utils import string_types

import warnings
import weakref

from twisted.application import service
from twisted.internet import defer
from twisted.python import log
from zope.interface import implementer

from buildbot import interfaces
from buildbot.data import resultspec
from buildbot.process import buildrequest
from buildbot.process import workerforbuilder
from buildbot.process.build import Build
from buildbot.process.results import SUCCESS, WARNINGS, FAILURE, RETRY
from buildbot.util import service as util_service
from buildbot.util import ascii2unicode
from buildbot.util import epoch2datetime
from buildbot.worker_transition import WorkerAPICompatMixin
from buildbot.worker_transition import deprecatedWorkerClassMethod
from buildbot.worker_transition import deprecatedWorkerModuleAttribute


def enforceChosenWorker(bldr, workerforbuilder, breq):
    if 'workername' in breq.properties:
        workername = breq.properties['workername']
        if isinstance(workername, string_types):
            return workername == workerforbuilder.worker.workername

    return True


deprecatedWorkerModuleAttribute(locals(), enforceChosenWorker)


class Builder(util_service.ReconfigurableServiceMixin,
              service.MultiService,
              WorkerAPICompatMixin):

    # reconfigure builders before workers
    reconfig_priority = 196

    @property
    def expectations(self):
        warnings.warn("'Builder.expectations' is deprecated.")
        return None

    def __init__(self, name):
        service.MultiService.__init__(self)
        self.name = name

        # this is filled on demand by getBuilderId; don't access it directly
        self._builderid = None

        # build/wannabuild slots: Build objects move along this sequence
        self.building = []
        # old_building holds active builds that were stolen from a predecessor
        self.old_building = weakref.WeakKeyDictionary()

        # workers which have connected but which are not yet available.
        # These are always in the ATTACHING state.
        self.attaching_workers = []
        self._registerOldWorkerAttr("attaching_workers")

        # workers at our disposal. Each WorkerForBuilder instance has a
        # .state that is IDLE, PINGING, or BUILDING. "PINGING" is used when a
        # Build is about to start, to make sure that they're still alive.
        self.workers = []
        self._registerOldWorkerAttr("workers")

        self.config = None
        self.builder_status = None

        # Upstreams
        self.upstreams = set()

        # Downstreams (calculated by upstreams)
        self.downstreams = set()

        # Incomplete sourcestamps for dependent builds.
        # They can be removed if a build is completed successfully.
        self.incomplete_ssids = set()

        # Bisector
        self.bisect_ss = []

        # Dubious ssdis (per upstream builder).
        # They might be failures or hidde-failures.
        self.dubious_ssids = {}

    @defer.inlineCallbacks
    def reconfigServiceWithBuildbotConfig(self, new_config):
        # find this builder in the config
        for builder_config in new_config.builders:
            if builder_config.name == self.name:
                found_config = True
                break
        assert found_config, "no config found for builder '%s'" % self.name

        # set up a builder status object on the first reconfig
        if not self.builder_status:
            self.builder_status = self.master.status.builderAdded(
                name=builder_config.name,
                basedir=builder_config.builddir,
                tags=builder_config.tags,
                description=builder_config.description)

        self.config = builder_config

        # allocate  builderid now, so that the builder is visible in the web
        # UI; without this, the builder wouldn't appear until it preformed a
        # build.
        builderid = yield self.getBuilderId()

        self.master.data.updates.updateBuilderInfo(builderid,
                                                   builder_config.description,
                                                   builder_config.tags)

        self.builder_status.setDescription(builder_config.description)
        self.builder_status.setTags(builder_config.tags)
        self.builder_status.setWorkernames(self.config.workernames)
        self.builder_status.setCacheSize(new_config.caches['Builds'])

        # if we have any workers attached which are no longer configured,
        # drop them.
        new_workernames = set(builder_config.workernames)
        self.workers = [w for w in self.workers
                        if w.worker.workername in new_workernames]

        self.upstreams = set()
        if builder_config.upstreams:
            for bn in builder_config.upstreams:
                # FIXME: Check nonexist bn
                builder = self.master.botmaster.builders[bn]
                self.upstreams.add(builder)

                # Prepare dubious_ssids
                if builder not in self.dubious_ssids:
                    self.dubious_ssids[builder] = set()

                # Register downstream
                # FIXME: Is it reconfigurable?
                builder.downstreams.add(self)

    def __repr__(self):
        return "<Builder '%r' at %d>" % (self.name, id(self))

    def getBuilderIdForName(self, name):
        # buildbot.config should ensure this is already unicode, but it doesn't
        # hurt to check again
        name = ascii2unicode(name)
        return self.master.data.updates.findBuilderId(name)

    def getBuilderId(self):
        # since findBuilderId is idempotent, there's no reason to add
        # additional locking around this function.
        if self._builderid:
            return defer.succeed(self._builderid)

        d = self.getBuilderIdForName(self.name)

        @d.addCallback
        def keep(builderid):
            self._builderid = builderid
            return builderid
        return d

    @defer.inlineCallbacks
    def getOldestRequestTime(self):
        """Returns the submitted_at of the oldest unclaimed build request for
        this builder, or None if there are no build requests.

        @returns: datetime instance or None, via Deferred
        """
        bldrid = yield self.getBuilderId()
        unclaimed = yield self.master.data.get(
            ('builders', bldrid, 'buildrequests'),
            [resultspec.Filter('claimed', 'eq', [False])])
        if unclaimed:
            unclaimed = sorted([brd['submitted_at'] for brd in unclaimed])
            defer.returnValue(unclaimed[0])
        else:
            defer.returnValue(None)

    def getBuild(self, number):
        for b in self.building:
            if b.build_status and b.build_status.number == number:
                return b
        for b in self.old_building:
            if b.build_status and b.build_status.number == number:
                return b
        return None

    def addLatentWorker(self, worker):
        assert interfaces.ILatentWorker.providedBy(worker)
        for w in self.workers:
            if w == worker:
                break
        else:
            wfb = workerforbuilder.LatentWorkerForBuilder(worker, self)
            self.workers.append(wfb)
            self.botmaster.maybeStartBuildsForBuilder(self.name)
    deprecatedWorkerClassMethod(locals(), addLatentWorker)

    def attached(self, worker, commands):
        """This is invoked by the Worker when the self.workername bot
        registers their builder.

        @type  worker: L{buildbot.worker.Worker}
        @param worker: the Worker that represents the worker as a whole
        @type  commands: dict: string -> string, or None
        @param commands: provides the worker's version of each RemoteCommand

        @rtype:  L{twisted.internet.defer.Deferred}
        @return: a Deferred that fires (with 'self') when the worker-side
                 builder is fully attached and ready to accept commands.
        """
        for w in self.attaching_workers + self.workers:
            if w.worker == worker:
                # already attached to them. This is fairly common, since
                # attached() gets called each time we receive the builder
                # list from the worker, and we ask for it each time we add or
                # remove a builder. So if the worker is hosting builders
                # A,B,C, and the config file changes A, we'll remove A and
                # re-add it, triggering two builder-list requests, getting
                # two redundant calls to attached() for B, and another two
                # for C.
                #
                # Therefore, when we see that we're already attached, we can
                # just ignore it.
                return defer.succeed(self)

        wfb = workerforbuilder.WorkerForBuilder()
        wfb.setBuilder(self)
        self.attaching_workers.append(wfb)
        d = wfb.attached(worker, commands)
        d.addCallback(self._attached)
        d.addErrback(self._not_attached, worker)
        return d

    def _attached(self, wfb):
        self.attaching_workers.remove(wfb)
        self.workers.append(wfb)

        return self

    def _not_attached(self, why, worker):
        # already log.err'ed by WorkerForBuilder._attachFailure
        # TODO: remove from self.workers (except that detached() should get
        #       run first, right?)
        log.err(why, 'worker failed to attach')

    def detached(self, worker):
        """This is called when the connection to the bot is lost."""
        for wfb in self.attaching_workers + self.workers:
            if wfb.worker == worker:
                break
        else:
            log.msg("WEIRD: Builder.detached(%s) (%s)"
                    " not in attaching_workers(%s)"
                    " or workers(%s)" % (worker, worker.workername,
                                         self.attaching_workers,
                                         self.workers))
            return

        if wfb in self.attaching_workers:
            self.attaching_workers.remove(wfb)
        if wfb in self.workers:
            self.workers.remove(wfb)

        # inform the WorkerForBuilder that their worker went away
        wfb.detached()

    def getAvailableWorkers(self):
        return [wfb for wfb in self.workers if wfb.isAvailable()]
    deprecatedWorkerClassMethod(locals(), getAvailableWorkers)

    def canStartWithWorkerForBuilder(self, workerforbuilder):
        locks = [(self.botmaster.getLockFromLockAccess(access), access)
                 for access in self.config.locks]
        return Build.canStartWithWorkerForBuilder(locks, workerforbuilder)
    deprecatedWorkerClassMethod(locals(), canStartWithWorkerForBuilder,
                                compat_name="canStartWithSlavebuilder")

    def canStartBuild(self, workerforbuilder, breq):
        if callable(self.config.canStartBuild):
            return defer.maybeDeferred(self.config.canStartBuild, self, workerforbuilder, breq)
        return defer.succeed(True)

    def _startBuildFor(self, workerforbuilder, buildrequests):
        build = self.config.factory.newBuild(buildrequests)
        build.setBuilder(self)
        build.setupProperties()
        log.msg("starting build %s using worker %s" %
                (build, workerforbuilder))

        # set up locks
        build.setLocks(self.config.locks)

        if self.config.env:
            build.setWorkerEnvironment(self.config.env)

        # append the build to self.building
        self.building.append(build)

        # The worker is ready to go. workerforbuilder.buildStarted() sets its
        # state to BUILDING (so we won't try to use it for any other builds).
        # This gets set back to IDLE by the Build itself when it finishes.
        # Note: This can't be done in `Build.startBuild`, since it needs to be done
        # synchronously, before the BuildRequestDistributor looks at
        # another build request.
        workerforbuilder.buildStarted()

        # create the BuildStatus object that goes with the Build
        bs = self.builder_status.newBuild()

        # let status know
        self.master.status.build_started(buildrequests[0].id, self.name, bs)

        # start the build. This will first set up the steps, then tell the
        # BuildStatus that it has started, which will announce it to the world
        # (through our BuilderStatus object, which is its parent).  Finally it
        # will start the actual build process.  This is done with a fresh
        # Deferred since _startBuildFor should not wait until the build is
        # finished.  This uses `maybeDeferred` to ensure that any exceptions
        # raised by startBuild are treated as deferred errbacks (see
        # http://trac.buildbot.net/ticket/2428).
        d = defer.maybeDeferred(build.startBuild,
                                bs, workerforbuilder)
        # this shouldn't happen. if it does, the worker will be wedged
        d.addErrback(log.err, 'from a running build; this is a '
                     'serious error - please file a bug at http://buildbot.net')

        return defer.succeed(True)

    def setupProperties(self, props):
        props.setProperty("buildername", self.name, "Builder")
        if self.config.properties:
            for propertyname in self.config.properties:
                props.setProperty(propertyname,
                                  self.config.properties[propertyname],
                                  "Builder")

    def buildFinished(self, build, wfb):
        """This is called when the Build has finished (either success or
        failure). Any exceptions during the build are reported with
        results=FAILURE, not with an errback."""

        # by the time we get here, the Build has already released the worker,
        # which will trigger a check for any now-possible build requests
        # (maybeStartBuilds)

        results = build.build_status.getResults()

        self.building.remove(build)
        if results == RETRY:
            d = self._resubmit_buildreqs(build)
            d.addErrback(log.err, 'while resubmitting a build request')
        else:
            complete_at_epoch = self.master.reactor.seconds()
            complete_at = epoch2datetime(complete_at_epoch)
            brids = [br.id for br in build.requests]

            d = self.master.data.updates.completeBuildRequests(
                brids, results, complete_at=complete_at)
            # nothing in particular to do with this deferred, so just log it if
            # it fails..
            d.addErrback(log.err, 'while marking build requests as completed')

        if wfb.worker:
            wfb.worker.releaseLocks()

        if results in (SUCCESS, WARNINGS):
            s = [ss.ssid for ss in build.sourcestamps]
            self.incomplete_ssids.difference_update(s)
            fFailToSucc = False
            for ssid in sorted(self.incomplete_ssids):
                if ssid > s[-1]:
                    break
                # It means Fail->Succ
                fFailToSucc = True
                self.incomplete_ssids.remove(ssid)

            for builder in self.downstreams:
                if fFailToSucc:
                    # Add ssids but s[-1], since it may be successful.
                    builder.dubious_ssids[self] |= set(s[0:-1])
                else:
                    # Prune ever.
                    for ssid in list(builder.dubious_ssids[self]):
                        if ssid <= s[-1]:
                            builder.dubious_ssids[self].discard(ssid)

        else:
            for builder in self.downstreams:
                builder.dubious_ssids[self] |= set([ss.ssid for ss in build.sourcestamps])

        if self.bisect_ss and build.reason == 'bisect':
            if results == SUCCESS or results == WARNINGS:
                # Prune successful sourcestamps (in head)
                a = []
                s = set([ss.ssid for ss in build.sourcestamps])
                for ss in self.bisect_ss:
                    if ss.ssid not in s:
                        a.append(ss)
                self.bisect_ss = a
                log.msg("********BISECT/SUCC, pruning %d, left %d" % (len(s), len(self.bisect_ss)))
            elif results == FAILURE:
                # Overwrite blamelist with the last build
                self.bisect_ss = build.sourcestamps
                log.msg("********BISECT/FAIL, left %d" % len(self.bisect_ss))
            else:
                log.msg("********BISECT: Results is <%s>. Aborting." % str(results))
                self.bisect_ss = []
        elif results == FAILURE and not self.bisect_ss and len(build.sourcestamps) >= 1:
            log.msg("********FAILURE: START BISECT********(%d)" % len(build.sourcestamps))
            self.bisect_ss = build.sourcestamps

        if self.bisect_ss:
            blamelist = []
            last_who = ''

            # Construct consequent blamelist
            for ss in self.bisect_ss:
                assert len(ss.changes) >= 1
                # Assuming change is one in each ss.
                ch = ss.changes[0]
                if last_who != ch.who:
                    last_who = ch.who
                    blamelist.append([])
                blamelist[-1].append(ss)

            # Construct the next build
            assert len(blamelist) > 0
            next_bisect_ss = []
            if len(blamelist) > 1:
                # Bisect by author
                n = (len(blamelist) + 1) // 2
                for bu in blamelist[0:n]:
                    next_bisect_ss += bu
                log.msg("BISECTING by AUTHORS: %d" % len(next_bisect_ss))
            else:
                ss = blamelist[0]
                assert len(ss) > 0
                if len(ss) > 1:
                    # Bisect by ss
                    n = (len(ss) + 1) // 2
                    next_bisect_ss = ss[0:n]
                    log.msg("BISECTING by ss: %d" % len(next_bisect_ss))
                elif results in (SUCCESS, WARNINGS):
                    # Run the one ss to confirm BAD.
                    next_bisect_ss = ss
                    log.msg("BISECTING(CONRIFM) by ss: %d" % len(next_bisect_ss))
                else:
                    log.msg("BISECTING, COMPLETE -- no need to run any more")
                    self.bisect_ss = []

            ssids = [ss.ssid for ss in next_bisect_ss]
            if len(ssids) > 0:
                self._submit_ssids(ssids, [build.requests[0].builderid])
        else:
            log.msg("---- not bis")

    @defer.inlineCallbacks
    def _submit_ssids(self, ssids, builderids):
        res = yield self.master.data.updates.addBuildset(
            waited_for=False, scheduler=u'bisect',
            sourcestamps=ssids, reason=u'bisect',
            builderids = builderids,
        )
        defer.returnValue(res)

    def _resubmit_buildreqs(self, build):
        brids = [br.id for br in build.requests]
        d = self.master.data.updates.unclaimBuildRequests(brids)

        @d.addCallback
        def notify(_):
            pass  # XXX method does not exist
            # self._msg_buildrequests_unclaimed(build.requests)
        return d

    # Build Creation

    def maybeStartBuild(self, workerforbuilder, breqs):
        # This method is called by the botmaster whenever this builder should
        # start a set of buildrequests on a worker. Do not call this method
        # directly - use master.botmaster.maybeStartBuildsForBuilder, or one of
        # the other similar methods if more appropriate

        # first, if we're not running, then don't start builds; stopService
        # uses this to ensure that any ongoing maybeStartBuild invocations
        # are complete before it stops.
        if not self.running:
            return defer.succeed(False)

        # If the build fails from here on out (e.g., because a worker has failed),
        # it will be handled outside of this function. TODO: test that!

        return self._startBuildFor(workerforbuilder, breqs)

    # a few utility functions to make the maybeStartBuild a bit shorter and
    # easier to read

    def getCollapseRequestsFn(self):
        """Helper function to determine which collapseRequests function to use
        from L{_collapseRequests}, or None for no merging"""
        # first, seek through builder, global, and the default
        collapseRequests_fn = self.config.collapseRequests
        if collapseRequests_fn is None:
            collapseRequests_fn = self.master.config.collapseRequests
        if collapseRequests_fn is None:
            collapseRequests_fn = True

        # then translate False and True properly
        if collapseRequests_fn is False:
            collapseRequests_fn = None
        elif collapseRequests_fn is True:
            collapseRequests_fn = self._defaultCollapseRequestFn

        return collapseRequests_fn

    @staticmethod
    def _defaultCollapseRequestFn(master, builder, brdict1, brdict2):
        return buildrequest.BuildRequest.canBeCollapsed(master, brdict1, brdict2)


@implementer(interfaces.IBuilderControl)
class BuilderControl:

    def __init__(self, builder, control):
        self.original = builder
        self.control = control

    @defer.inlineCallbacks
    def getPendingBuildRequestControls(self):
        master = self.original.master
        # TODO Use DATA API
        brdicts = yield master.db.buildrequests.getBuildRequests(
            buildername=self.original.name,
            claimed=False)

        # convert those into BuildRequest objects
        buildrequests = []
        for brdict in brdicts:
            br = yield buildrequest.BuildRequest.fromBrdict(
                self.control.master, brdict)
            buildrequests.append(br)

        # and return the corresponding control objects
        defer.returnValue([buildrequest.BuildRequestControl(self.original, r)
                           for r in buildrequests])

    def getBuild(self, number):
        return self.original.getBuild(number)

    def ping(self):
        if not self.original.workers:
            return defer.succeed(False)  # interfaces.NoWorkerError
        dl = []
        for w in self.original.workers:
            dl.append(w.ping(self.original.builder_status))
        d = defer.DeferredList(dl)
        d.addCallback(self._gatherPingResults)
        return d

    def _gatherPingResults(self, res):
        for ignored, success in res:
            if not success:
                return False
        return True
