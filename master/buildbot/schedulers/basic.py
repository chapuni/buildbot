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

from twisted.internet import defer, reactor
from twisted.python import log
from buildbot import util, config
from buildbot.util import NotABranch
from collections import defaultdict
from buildbot.changes import filter, changes
from buildbot.schedulers import base, dependent
from buildbot.status.results import SUCCESS, WARNINGS

class BaseBasicScheduler(base.BaseScheduler):
    """
    @param onlyImportant: If True, only important changes will be added to the
                          buildset.
    @type onlyImportant: boolean

    """

    compare_attrs = (base.BaseScheduler.compare_attrs +
                     ('treeStableTimer', 'change_filter', 'fileIsImportant',
                      'onlyImportant') )

    _reactor = reactor # for tests

    fileIsImportant = None

    class NotSet: pass
    def __init__(self, name, shouldntBeSet=NotSet, treeStableTimer=None,
                builderNames=None, branch=NotABranch, branches=NotABranch,
                fileIsImportant=None, properties={}, categories=None,
                upstreams=None,
                change_filter=None, onlyImportant=False, **kwargs):
        if shouldntBeSet is not self.NotSet:
            config.error(
                "pass arguments to schedulers using keyword arguments")
        if fileIsImportant and not callable(fileIsImportant):
            config.error(
                "fileIsImportant must be a callable")

        # initialize parent classes
        base.BaseScheduler.__init__(self, name, builderNames, properties, **kwargs)

        self.treeStableTimer = treeStableTimer
        if fileIsImportant is not None:
            self.fileIsImportant = fileIsImportant
        self.onlyImportant = onlyImportant
        self.change_filter = self.getChangeFilter(branch=branch,
                branches=branches, change_filter=change_filter,
                categories=categories)
        self.buildernames = {}
        self.pendings = {}
        self.upstreams = upstreams
        self._buildset_addition_subscr = None
        self._buildset_completion_subscr = None

        # the IDelayedCall used to wake up when this scheduler's
        # treeStableTimer expires.
        self._stable_timers = defaultdict(lambda : None)
        self._stable_timers_lock = defer.DeferredLock()

        # the subscription lock makes sure that we're done inserting a
        # subcription into the DB before registering that the buildset is
        # complete.
        self._subscription_lock = defer.DeferredLock()

    def getChangeFilter(self, branch, branches, change_filter, categories):
        raise NotImplementedError

    def startService(self, _returnDeferred=False):
        base.BaseScheduler.startService(self)

        if self.upstreams:
            self._buildset_addition_subscr = \
                self.master.subscribeToBuildsets(self._buildsetAdded)
            self._buildset_completion_subscr = \
                self.master.subscribeToBuildsetCompletions(self._buildsetCompleted)
        d = self.startConsumingChanges(fileIsImportant=self.fileIsImportant,
                                       change_filter=self.change_filter,
                                       onlyImportant=self.onlyImportant)

        # if treeStableTimer is False, then we don't care about classified
        # changes, so get rid of any hanging around from previous
        # configurations
        if False and not self.treeStableTimer:
            d.addCallback(lambda _ :
                self.master.db.schedulers.flushChangeClassifications(
                                                        self.objectid))

        # otherwise, if there are classified changes out there, start their
        # treeStableTimers again
        else:
            d.addCallback(lambda _ :
                self.scanExistingClassifiedChanges())

        # handle Deferred errors, since startService does not return a Deferred
        d.addErrback(log.err, "while starting SingleBranchScheduler '%s'"
                              % self.name)

        if _returnDeferred:
            return d # only used in tests

    def stopService(self):
        if self._buildset_addition_subscr:
            self._buildset_addition_subscr.unsubscribe()
            self._buildset_addition_subscr = None
        if self._buildset_completion_subscr:
            self._buildset_completion_subscr.unsubscribe()
            self._buildset_completion_subscr = None
        # the base stopService will unsubscribe from new changes
        d = base.BaseScheduler.stopService(self)
        @util.deferredLocked(self._stable_timers_lock)
        def cancel_timers(_):
            for timer in self._stable_timers.values():
                if timer:
                    timer.cancel()
            self._stable_timers.clear()
        d.addCallback(cancel_timers)
        return d

    @util.deferredLocked('_stable_timers_lock')
    def gotChange(self, change, important):
        timer_name = change.branch # FIXME!

        # Demote a change unimportant, if either of upstreams accepts it.
        pending = False
        if important and not self.treeStableTimer:
            if not self.pendings.has_key(timer_name):
                self.pendings[timer_name] = {}
            p = self.pendings[timer_name]
            if not p.has_key(change.revision):
                p[change.number] = {
                    'builders': {},
                    'revision': change.revision,
                    'change': change,
                    }
            if self.upstreams:
                for ups in self.upstreams:
                    if not ups.change_filter or ups.change_filter.filter_change(change):
                        # Inactivate this until completed.
                        pending = True
                        p[change.numbers]['builders'][ups.builderNames[0]] = True

        if not important:
            pending = True

        if not self.treeStableTimer:
            # if there's no treeStableTimer, we can completely ignore
            # unimportant changes
            # FIXME: Don't ignore and do classify it if we could blame it.
            if not important:
                return defer.succeed(None)

            d = self.master.db.schedulers.classifyChanges(
                self.objectid, { change.number : important })
            if not pending:
                d.addCallback(lambda _: self.aaa(timer_name))
            return d

        timer_name = self.getTimerNameForChange(change)

        # if we have a treeStableTimer, then record the change's importance
        # and:
        # - for an important change, start the timer
        # - for an unimportant change, reset the timer if it is running
        d = self.master.db.schedulers.classifyChanges(
                self.objectid, { change.number : important })
        def fix_timer(_):
            if not important and not self._stable_timers[timer_name]:
                return
            if self._stable_timers[timer_name]:
                self._stable_timers[timer_name].cancel()
            def fire_timer():
                d = self.stableTimerFired(timer_name)
                d.addErrback(log.err, "while firing stable timer")
            self._stable_timers[timer_name] = self._reactor.callLater(
                    self.treeStableTimer, fire_timer)
        d.addCallback(fix_timer)
        return d

    @defer.inlineCallbacks
    def scanExistingClassifiedChanges(self):
        # call gotChange for each classified change.  This is called at startup
        # and is intended to re-start the treeStableTimer for any changes that
        # had not yet been built when the scheduler was stopped.

        # NOTE: this may double-call gotChange for changes that arrive just as
        # the scheduler starts up.  In practice, this doesn't hurt anything.
        classifications = \
                yield self.master.db.schedulers.getChangeClassifications(
                                                                self.objectid)

        # call gotChange for each change, after first fetching it from the db
        for changeid, important in classifications.iteritems():
            chdict = yield self.master.db.changes.getChange(changeid)

            if not chdict:
                continue

            change = yield changes.Change.fromChdict(self.master, chdict)
            yield self.gotChange(change, important)

    def getTimerNameForChange(self, change):
        raise NotImplementedError # see subclasses

    def getChangeClassificationsForTimer(self, objectid, timer_name):
        """similar to db.schedulers.getChangeClassifications, but given timer
        name"""
        raise NotImplementedError # see subclasses

    @util.deferredLocked('_stable_timers_lock')
    @defer.inlineCallbacks
    def stableTimerFired(self, timer_name):
        # if the service has already been stoppd then just bail out
        if not self._stable_timers[timer_name]:
            return

        # delete this now-fired timer
        del self._stable_timers[timer_name]

        classifications = \
            yield self.getChangeClassificationsForTimer(self.objectid,
                                                            timer_name)

        # just in case: databases do weird things sometimes!
        if not classifications: # pragma: no cover
            return

        changeids = sorted(classifications.keys())
        yield self.addBuildsetForChanges(reason='scheduler',
                                           changeids=changeids)

        max_changeid = changeids[-1] # (changeids are sorted)
        yield self.master.db.schedulers.flushChangeClassifications(
                            self.objectid, less_than=max_changeid+1)

    def aaa(self, key):
        # Get changes (from scheduler_changes)
        classifications = \
            yield self.getChangeClassificationsForTimer(self.objectid,
                                                            timer_name)

        # just in case: databases do weird things sometimes!
        if not classifications: # pragma: no cover
            return

        print "****KEY=", key

        changeids = sorted(classifications.keys(), reversed=True)
        print "****ORIG_changeids=", changeids

        # Seek the latest change (in reverse) to graduate.
        max_i = -1
        if self.pendings.has_key(timer_name):
            p = self.pendings[timer_name]
            for i, changeid in enumerate(changeids):
                if p.has_key(changeid):
                    if max_i < 0 and len(p[changeid]['builders']) == 0:
                        max_i = changeid
                    delete p[changeid]

        if max_i < 0:
            return

        changeids = reversed(changeids[:max_i])

        print "****GRADUATED: ", changeids

        yield self.addBuildsetForChanges(reason='scheduler',
                                           changeids=changeids)

        max_changeid = changeids[-1] # (changeids are sorted)
        # FIXME: Does it flush changes in other branches?
        yield self.master.db.schedulers.flushChangeClassifications(
                            self.objectid, less_than=max_changeid+1)

    def getPendingBuildTimes(self):
        # This isn't locked, since the caller expects and immediate value,
        # and in any case, this is only an estimate.
        return [timer.getTime() for timer in self._stable_timers.values() if timer and timer.active()]

    @util.deferredLocked('_subscription_lock')
    def _buildsetAdded(self, bsid=None, properties=None, **kwargs):
        # Assumption: This callback has an argument 'builderNames'.
        if not kwargs.has_key('builderNames'):
            return
        # For now, a scheduler is assumed to fire at most one builder.
        if len(kwargs['builderNames']) != 1:
            return
        for x in kwargs['builderNames']:
            buildername = x
        # Look into builder rather than scheduler.
        for ups in self.upstreams:
            if buildername in ups.builderNames:
                self.buildernames[bsid] = buildername
                # TODO: Suspend timer.
                return

    def _buildsetCompleted(self, bsid, result):
        d = self._checkCompletedBuildsets(bsid, result)
        d.addErrback(log.err, 'while checking for completed buildsets')

    @util.deferredLocked('_subscription_lock')
    @defer.inlineCallbacks
    def _checkCompletedBuildsets(self, bsid, result):
        print "****<%s>****" % self.name
        print "****BSID: ", bsid
        print "********PEND: ", self.pendings
        print "****master: ", self.master
        print "****RESULT: ", result

        # For now, leave changes pending when result was not successful.
        if result not in (SUCCESS, WARNINGS):
            yield defer.succeed(None)
            return

        # Oh no, I don't know such a bsid, ... do nothing.
        if not self.buildernames.has_key(bsid):
            yield defer.succeed(None)
            return

        print "****buildernames: ", self.buildernames
        buildername = self.buildernames[bsid]
        del self.buildernames[bsid]
        bsdict = yield self.master.db.buildsets.getBuildset(bsid) # exceptions.AttributeError: 'NoneType' object has no attribute 'db'
        sss = yield self.master.db.sourcestamps.getSourceStamps(bsdict['sourcestampsetid'])
        print "****BuilderName: ", buildername
        print "****BS: ", bsdict
        print "***SSS: ", sss

        found = False
        for ss in sss:
            branch = ss['branch']
            rev = ss['revision']
            for changeid, p in enumerate(p[branch]):
                if p['change'].revision == rev and p['builders'].has_key(buildername):
                    del p['builders'][buildername]
                    found = True
                    break

        if not found:
            yield defer.succeed(None)
            return

        yield self.aaa(branch)
        return

class SingleBranchScheduler(BaseBasicScheduler):
    def getChangeFilter(self, branch, branches, change_filter, categories):
        if branch is NotABranch and not change_filter:
            config.error(
                "the 'branch' argument to SingleBranchScheduler is " +
                "mandatory unless change_filter is provided")
        elif branches is not NotABranch:
            config.error(
                "the 'branches' argument is not allowed for " +
                "SingleBranchScheduler")


        return filter.ChangeFilter.fromSchedulerConstructorArgs(
                change_filter=change_filter, branch=branch,
                categories=categories)

    def getTimerNameForChange(self, change):
        return "only" # this class only uses one timer

    def getChangeClassificationsForTimer(self, objectid, timer_name):
        return self.master.db.schedulers.getChangeClassifications(
                                                        self.objectid)


class Scheduler(SingleBranchScheduler):
    "alias for SingleBranchScheduler"
    def __init__(self, *args, **kwargs):
        log.msg("WARNING: the name 'Scheduler' is deprecated; use " +
                "buildbot.schedulers.basic.SingleBranchScheduler instead " +
                "(note that this may require you to change your import " +
                "statement)")
        SingleBranchScheduler.__init__(self, *args, **kwargs)


class AnyBranchScheduler(BaseBasicScheduler):
    def getChangeFilter(self, branch, branches, change_filter, categories):
        assert branch is NotABranch
        return filter.ChangeFilter.fromSchedulerConstructorArgs(
                change_filter=change_filter, branch=branches,
                categories=categories)

    def getTimerNameForChange(self, change):
        # Py2.6+: could be a namedtuple
        return (change.codebase, change.project, change.repository, change.branch)

    def getChangeClassificationsForTimer(self, objectid, timer_name):
        codebase, project, repository, branch = timer_name # set in getTimerNameForChange
        return self.master.db.schedulers.getChangeClassifications(
                self.objectid, branch=branch, repository=repository,
                codebase=codebase, project=project)

# now at buildbot.schedulers.dependent, but keep the old name alive
Dependent = dependent.Dependent
