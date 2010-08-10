"""
Copyright (c) 2010 Yariv Sadan

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

from google.appengine.api import memcache
from google.appengine.ext import db
from asynctools import AsyncMultiTask, QueryTask
import logging
import types
from functools import partial

class DataFetcherListener:
    def on_fetch(self, requests):
        pass
    
    def on_db_request(self, request):
        pass

    def on_db_fetch(self):
        pass

class DataFetcher:
    """
    DataFetcher is a class that simplifies querying the data store
    with maximal query parallelism and cache utilization.

    It requires the asynctools package, which you can obtain at
    http://code.google.com/p/asynctools/.

    When used properly, DataFetcher ensures the following:

    - All the data fetches that could be parallelized are parallelized (both
      when fetching from the data store and from memcache).
    - All fetched data is stored in memcache and in a local per process cache.
      The data for a given cachekey is only fetched from memcache if it's
      missing from the local cache, and it's only fetched from the data store
      if it's missing from memcache.
    - Simultaneous requests for the same cache key are combined into a single
      request.


    A simple example showing how to fetch a single object:
        
    def request_cat(fetcher):
      ref = fetcher.request_obj(Cat, cat_id)
      yield ref.get
          
    fetcher = DataFetcher()
    requester_ref = spawn(requester_cat)
    fetcher.run()
    cat = requester_ref.get()


    A more complex example, showing how to fetch multiple related objects:

    def requester(cat_id, fetcher):
      cat_ref = fetcher.request_obj(Cat, cat_id)
      yield
      cat = cat_ref.get()
      friend_id = cat.friend_id
      friend_ref = fetcher.request_obj(Cat, friend_id)

      def ondone():
        cat.friend = friend_ref.get()
        return cat

      yield ondone

    cat_ids = [123, 456]
    requesters = [functools.partial(requester, id) for id in cat_ids]
    fetcher = DataFetcher()
    refs = fetcher.spawn_multi(requesters)
    fetcher.run()

    cats = [ref.get() for ref in refs]
    friends = [cat.friend for cat in cat]

      
    For an overview on how to use it please read the accompanying
    README.txt file or see the examples in the unit tests.
    """

    # A local cache of the data fetched from the data store and/or
    # memcache. It maps memcache key to the fetched data.
    # This cache is shared by all DataFetcher instances in the process
    # so that the data will be fetched at most once per process.
    fetched_data = {}

    # Contains pending data requests. All DataFetcher instances
    # share this global requests dictionary, which maps memcache keys
    # to _DataRequest objects.
    requests = {}

    # A global listener for data fetching events. It's useful mostly
    # for diagnostics during testing.
    listener = DataFetcherListener()
    
    @classmethod
    def set_listener(cls, listener):
        """ Sets the global DataFetcherListener """
        cls.listener = listener

    @classmethod
    def get_listener(cls):
        """ Gets the global DataFetcherListener """
        return cls.listener

    def __init__(self):
        # A list of _RequesterData objects, containing requesters
        # added using spawn() and their corresponding Refs.
        self.requester_datas = []

        # Similar to self.requester_datas, but contains requesters added
        # using spawn_and_join()
        self.joined_requester_datas = []

        # A list of DoneRequester objects containing callable objects
        # returned from the requesters via 'yield callable' and the refs
        # from the corresponding RequesterData objects.
        # When the DataFetcher is done fetching data, is calls the
        # callables and stores their results in the refs.
        self.done_requesters = []

        # Indicates if the DataFetcher was run
        self.did_run = False

        # Indicates if the DataFetcher is running
        self.is_running = False

    def spawn(self, requester):
        """
        Adds the requester function to the list of functions
        that should be executed in the DataFetcher's main "thread".

        The requester function should take a single parameter, which is
        the DataFetcher object that runs the requester.

        Requester functions typically have a sequence of
        request() or request_obj() calls that request independent
        sets of data that can be fetched in parallel.
        If two or more data fetches depend on each other (for
        example, if a requester fetches an object and a related object
        whose id is only known once the first object has been fetched),
        the requester can call 'yield' between calls to request().
        After a call to 'yield', all the data from the previous
        calls to request() and request_obj() should have been
        fetched if no errors have occurred.

        In their last yield statement, requesters can return a callable object.
        The result of the callable object will be set as the value of the
        Ref object that spawn() returns. The callable is called after the
        DataFetcher is done fetching all the requested data.

        spawn() can be called from within a requester function
        (in other words, requesters can spawn other requesters).
        This is useful if a requester wants to ensure the data from
        another requester is fetched before the DataFetcher's
        main thread ends (but not necessarily before the calling requester's
        next pass -- see spawn_and_join() for a different approach).

        Tip: If you want to create a generic requester function that
        takes more than the single DataFetcher parameter, you can
        use functools.partial to bind values to the requester's extra
        parameters before spawning it.

        See the example at the top of the file for details.

        """
        
        return self.spawn_multi([requester])[0]

    def spawn_multi(self, requesters):
        """
        Similar to spawn(), but takes a list of requesters and returns
        a list of refs.
        """
        return self._add_new_requesters(self.requester_datas, requesters)

    def spawn_and_join(self, requester):
        """
        Similar to spawn(), but ensures that the requester will be executed
        fully before the calling requester's next pass.
        This function is meant to be called from within a requester
        when it depends on the completion of
        another requester before proceeding to the next pass.

        Example:

        def requester(fetcher):
          ref = fetcher.spawn_and_join(other_requester)
          yield
          data = ref.get() # contains the result of other_requester
          ref fetcher.request(...)
          yield ref.get

        result = DataFetcher().spawn_and_run(requester)
        """
        return self.spawn_and_join_multi([requester])[0]

    def spawn_and_join_multi(self, requesters):
        """
        Simliar to spawn_and_join, but takes a list of requesters
        and returns a list of refs.
        """
        return self._add_new_requesters(self.joined_requester_datas, requesters)

    def spawn_and_run(self, requester):
        """
        Spawns the requester, runs the fetcher, and returns the requester's
        result.
        """
        return self.spawn_and_run_multi([requester])[0]

    def spawn_and_run_multi(self, requesters):
        """
        Simliar to spawn_and_run, but takes multiple requesters and
        returns multiple results
        """
        refs = self.spawn_multi(requesters)
        self.run()
        return [ref.get() for ref in refs]
        
    def request(self, mkey, query, limit = 1):
        """
        This function adds a data request to the DataFetcher. This function is
        meant to be called from within a requester function (see spawn()
        and spawn_and_join() for more details). After the next 'yield' call
        within the requester, the requested data should be available.
        For convenience, this function returns a ref that will hold the
        requested data after the 'yield' call.

        If multiple requesters request the same data, it'll only be fetched
        once.

        Example:
        
        def requester(fetcher):
          ref = fetcher.request("cats", db.GqlQuery("select * from Cat"),
            limit = 10)
          yield
          cats = fetcher.get("cats")

          # another option:
          cats = ref.get()

          ref1 = fetcher.request(...)
          yield ref1.get

        DataFetcher.spawn_and_run(requester)
        """
        if not self.is_running:
            raise NotRunningException()

        ref = Ref()

        # If the data for memcache key has already been fetched
        # don't request it again.
        result = self.fetched_data.get(mkey)
        if result:
            
            # If the new request has a related Ref, set its
            # value to the prefetched data before returning
            ref.set(result)
            return ref

        # If the data for memcache key has already been requested
        # but not fetched, don't request it again.
        request = self.requests.get(mkey)
        if request:

            # add the ref to the existing request before returning
            request.add_ref(ref)
            return ref

        self.requests[mkey] = _DataRequest(mkey, query, limit, ref)
        return ref

    def request_obj(self, obj_cls, id):
        """
        Similar to request(), but takes an object class and and id.
        When requesting a single object, it's more conventient than
        using the data store query apis directly.
        
        See get_obj().
        """
        obj_cls_name = obj_cls.__name__
        ref = self.request(
            self._get_obj_mkey(obj_cls_name, id),
            db.GqlQuery('select * from %s where __key__=:1' % obj_cls_name,
                        db.Key.from_path(obj_cls_name, long(id))),
            limit = 1)
        return ref

    def get(self, mkey):
        """
        Returns the data for the previously fetched memcache key.
        This function can be called from within a requester.
        """
        return self.get_multi([mkey])[mkey]

    def get_multi(self, mkeys):
        """
        Similar to get(), but takes multiple keys and returns a list of
        results.
        """
        if not self.did_run:
            raise NotRunException()
        return dict([(mkey, DataFetcher.fetched_data[mkey]) for mkey in mkeys])
        

    def get_obj(self, obj_cls, ids):
        """
        After an object that has been requested with request_obj() has been
        fetched, get_obj() can be used for getting the object data.

        Example:

        def requester(fetcher):
          fetcher.request_obj(Cat, 1234)

        fetcher = DataFetcher()
        fetcher.spawn(requester)
        fetcher.run()
        obj = fetcher.get_obj(Cat, 123)

        # alternatively:
        def requester(fetcher):
          ref = fetcher.request_obj(Cat, 123)
          yield ref.get

        fetcher = DataFetcher()
        obj = fetcher.spawn_and_run(requester)

        """
        obj_cls_name = obj_cls.__name__
        if not type(ids) is list:
            mkey = self._get_obj_mkey(obj_cls_name, ids)
            return self.get(mkey)

        mkeys = [self._get_obj_mkey(obj_cls_name, id) for id in ids]
        return self.get(mkeys)


    @classmethod
    def delete(cls, *mkeys):
        """
        Deletes the memcache keys both from the local cache and from
        memcache. Use this instead of calling memcache.delete() directly.
        """
        for mkey in mkeys:
            if cls.fetched_data.has_key(mkey):
                del cls.fetched_data[mkey]
        memcache.delete_multi(mkeys)

    @classmethod
    def delete_obj_id(cls, obj_cls, id):
        mkey = self._get_obj_mkey(obj_cls, id)
        cls.delete(mkey)

    def delete_obj(cls, obj):
        return delete_obj_id(cls, obj.__class__, obj.key().id())

        
    @classmethod
    def fetch_obj(cls, obj_cls, id):
        """
        Fetches a single object and returns it immediately.
        This operation can't be parallelized with other
        fetches so its use is discouraged, but it can be convenient sometimes.

        Example:
        
        cat = DataFetcher.fetch_obj(Cat, 123)
        """
        res = cls.fetch_objs(obj_cls, [id])
        if res:
            res = res[0]
        return res

    @classmethod
    def fetch_objs(cls, obj_cls, ids):
        """
        Simliar to fetch_obj, but fetches multiple objects from the same
        class.
        """
        def requester(id, fetcher):
            ref = fetcher.request_obj(obj_cls, id)
            yield ref.get
        
        fetcher = DataFetcher()
        requesters = [partial(requester, id) for id in ids]
        return fetcher.spawn_and_run_multi(requesters)

    @classmethod
    def reset(cls):
        """
        Clears the global fetched data and pending requests containers
        """
        cls.fetched_data = {}
        cls.requests = {}

    def run(self):
        """
        Executes the DataFetcher's requesters. This function should ideally
        only be called once per process to maximize the potential for
        parallelism in data fetches.
        """
        if self.did_run:
            raise AlreadyRanException()

        DataFetcher.run_was_called = True
        self.did_run = True
        self.is_running = True

        while self.requester_datas or self.joined_requester_datas or self.requests:
            self._run_single_pass()


        # We need to reverse the list of done_requesters so that nested
        # calls to spawn are processed before their parents (another way
        # to think of it is as a stack of requesters, where each
        # nested call to spawn creates a new requester, which adds a
        # its callable to the top of the stack).
        for done_requester in reversed(self.done_requesters):
            done_requester.ref.set(done_requester.callable())

        self.is_running = False

        return self


    @classmethod
    def _get_obj_mkey(cls, obj_cls, id):
        """ Returns the cache key for an object """
        return '%s:%s' % (obj_cls, id)

    @staticmethod
    def _add_new_requesters(container, requesters):
        """ A helper method for adding new requesters to the DataFetcher """
        refs = []
        for requester in requesters:
            ref = Ref()
            rdata = _RequesterData(requester, ref)
            container.append(rdata)
            refs.append(ref)
        return refs


    def _run_single_pass(self):
        """
        Runs a single pass from all the pending requesters and fetches
        and their data requests in parallel.
        """
        
        # Holds the generators that the fetcher will process on the
        # next call to _run_single_pass
        remaining_requester_datas = []

        # The loop exists because each call to a requester
        # function could add new requester function to the
        # DataFetcher via spawn(). To maximize parallism
        # we process the first steps (before the call to yield)
        # of the newly added functions in the current pass until no new
        # functions are left.
        while self.requester_datas:
            requesters = self.requester_datas
            self.requester_datas = []
            requester_datas = self._call_generators(requesters)
            remaining_requester_datas.extend(requester_datas)

        self.requester_datas.extend(remaining_requester_datas)

        # If any functions need to be fetched immediately,
        # initialize a new DataFetcher for those functions
        # and run it. To maximize parallelism, make the
        # new data fetcher fetch any pending requests
        # from the current data fetcher on the new
        # data fetcher's first pass.
        if self.joined_requester_datas:
            extra_fetcher = DataFetcher()
            requesters = [requester_data.requester
                          for requester_data in self.joined_requester_datas]
            results = extra_fetcher.spawn_and_run_multi(requesters)
            for i in xrange(len(results)):
                self.joined_requester_datas[i].ref.set(results[i])
            self.joined_requester_datas = []
            return
        

        if DataFetcher.requests:
            results = self._fetch(DataFetcher.requests)
            self.fetched_data.update(results)
            for mkey, val in results.items():
                DataFetcher.requests[mkey].set_result(val)
            DataFetcher.requests = {}


    def _call_generators(self, requesters):
        remaining_requester_datas = []
        for requester_data in requesters:
            requester = requester_data.requester
            if not isinstance(requester, types.GeneratorType):
                
                return_value = requester(self)
                if return_value and isinstance(return_value,
                                               types.GeneratorType):
                    requester = return_value
                else:
                    continue

            try:
                res = requester.next()
                if callable(res):
                    self.done_requesters.append(
                        _DoneRequester(res, requester_data.ref))
                    requester.next()
                    raise Exception("'yield callable' must be the generator's"
                                    "last yield statement")
                else:
                    requester_data.requester = requester
                    remaining_requester_datas.append(requester_data)
            except StopIteration, e:
                pass
        return remaining_requester_datas


    @classmethod
    def _fetch(cls, requests):
        """
        Performs the memcache and data store fetches for requests
        whose data isn't in the local cache.
        """

        if not requests:
            return

        cls.listener.on_fetch(requests)

        mkeys = requests.keys()

        # first, try to fetch the keys from memcache
        results = memcache.get_multi(mkeys)

        # if any of the keys couldn't be fetched from
        # memcache, prepare async tasks to fetch
        # their queries from the data store
        runner = AsyncMultiTask()
        has_tasks = False

        # if there are any pending async tasks,
        # execute them using the AsyncMultiTask runner.
        db_fetches = {}

        for mkey, request in requests.items():
            if not results.get(mkey):
                task = QueryTask(request.query,
                                 limit = request.limit,
                                 client_state = mkey)
                runner.append(task)
                has_tasks = True

                cls.listener.on_db_request(request)

        if has_tasks:
            runner.run()
            cls.listener.on_db_fetch()
            for task in runner:
                val = task.get_result()
                mkey = task.client_state

                # if the task only tried to fetch a single object,
                # return the first element of the result list
                # or none
                if task.limit == 1:
                    if val:
                        val = val[0]
                    else:
                        val = None

                results[mkey] = val                
                db_fetches[mkey] = val

        # if any results were fetched from the data store,
        # store them in memcache
        if db_fetches:
            memcache.set_multi(db_fetches)

        return results

class Ref:
    """
    Holds a reference for a value.
    """
    def __init__(self):
        self.val = None

    def set(self, val):
        self.val = val

    def get(self):
        return self.val

class NotRunException(Exception):

    def __init__(self):
        super(NotRunException, self).__init__(
            "You must call run() before getting data from the fetcher!")

class NotRunningException(Exception):

    def __init__(self):
        super(NotRunningException, self).__init__(
            "You can only call request() from within a data fetching function!")

class AlreadyRanException(Exception):

    def __init__(self):
        super(AlreadyRanException, self).__init__(
            "The DataFetcher was already run!")

class _RequesterData:
    
    def __init__(self, requester, ref):
        self.requester = requester
        self.ref = ref

class _DoneRequester:
    def __init__(self, callable, ref):
        self.callable = callable
        self.ref = ref


class _DataRequest:
    def __init__(self, mkey, query, limit, ref):
        self.mkey = mkey
        self.query = query
        self.limit = limit
        self.refs = [ref]

    def add_ref(self, ref):
        self.refs.append(ref)

    def set_result(self, result):
        for ref in self.refs:
            ref.set(result)

