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

from data_fetcher.df import DataFetcher, DataFetcherListener, Ref, \
    NotRunException, NotRunningException, AlreadyRanException
from google.appengine.ext import db
from google.appengine.api import memcache
import logging
import random
import sys
from functools import partial
from utils.test import BaseTestCase

class DataFetcherTestListener(DataFetcherListener):
    
    def __init__(self):
        self.num_fetches = 0
        self.num_requests = 0
        self.num_db_requests = 0
        self.num_db_fetches = 0

    def on_fetch(self, requests):
        self.num_fetches = self.num_fetches + 1
        self.num_requests = self.num_requests + len(requests)

    def on_db_request(self, request):
        self.num_db_requests = self.num_db_requests + 1
    
    def on_db_fetch(self):
        self.num_db_fetches = self.num_db_fetches + 1

class Cat(db.Model):
    id = db.IntegerProperty(),
    name = db.StringProperty()
    friend_id = db.IntegerProperty()

    def get_mkey(self):
        return "Cat:" + str(self.key().id())

class TestDataFetcher(BaseTestCase):

    def setUp(self):
        DataFetcher.reset()
        DataFetcher.set_listener(DataFetcherTestListener())

    def assert_stats(self, num_fetches, num_requests, num_db_fetches):
        listener = DataFetcher.get_listener()
        self.assertEquals(num_fetches, listener.num_fetches)
        self.assertEquals(num_requests, listener.num_requests)
        self.assertEquals(num_db_fetches, listener.num_db_fetches)
        
    # Tests a single query that fetches a single object.
    def _test_single_query(self):
        cat = self.make_cat()
        def do_fetch():
            requester = partial(self.request_cat, cat)
            fetcher = DataFetcher()
            result = fetcher.spawn_and_run(requester)
            return (fetcher, result)

        fetcher, result = do_fetch()
        cat2 = fetcher.get(cat.get_mkey())
        self.assert_cat_equals(cat, cat2)
        self.assert_cat_equals(cat, result)
        self.do_test_cache(do_fetch, 1, 1, 1)

    # Tests multiple queries that fetch single objects
    def _test_multiple_single_queries(self):
        cats = [self.make_cat() for i in xrange(3)]
        def do_fetch():
            requesters = [partial(self.request_cat, cat)
                     for cat in cats]
            fetcher = DataFetcher()
            results = fetcher.spawn_and_run_multi(requesters)
            return (fetcher, results)

        fetcher, results = do_fetch()
        for i, cat in enumerate(cats):
            cat2 = fetcher.get(cat.get_mkey())
            self.assert_cat_equals(cat, cat2)
            self.assert_cat_equals(cat, results[i])

        self.do_test_cache(do_fetch, 1, 3, 1)

    # Tests that if two requests try to fetch the same memcache
    # key and they both use references, both references will contain
    # the result even though only one request will be fetched.
    # Test both parallel an sequential fetching of the same request
    # key.
    def _test_identical_requests_with_refs(self):
        cat = self.make_cat()

        requesters = [partial(self.request_cat, cat) for i in xrange(2)]

        cats = DataFetcher().spawn_and_run_multi(requesters)
        self.assert_stats(1, 1, 1)

        for cat1 in cats:
            self.assert_cat_equals(cat, cat1)

        # Next, test with a sequential request for the same key.
        requester = partial(self.request_cat, cat)
        cat1 = DataFetcher().spawn_and_run(requester)

        # check that the new data returned the data without performing any fetches
        # (the stats haven't changed)
        self.assert_stats(1, 1, 1)
        self.assert_cat_equals(cat, cat1)
        
    # Tests the request_obj and get_obj methods.
    def _test_obj_single_query(self):
        cat = self.make_cat()
        id = cat.key().id()

        def requester(fetcher):
            ref = fetcher.request_obj(Cat, id)
            yield ref.get

        fetcher = DataFetcher()
        cat2 = fetcher.spawn_and_run(requester)
        cat3 = fetcher.get_obj(Cat, id)

        self.assert_cat_equals(cat, cat3)
        self.assert_cat_equals(cat, cat2)
        self.assert_stats(1, 1, 1)

    # Tests a query that fetches multiple objects.
    def _test_multi_query(self):
        num = 3
        cats = [self.make_cat() for i in xrange(num)]

        mkey = "cats_test"
        memcache.delete(mkey)

        def requester(fetcher):
            query = db.GqlQuery("select * from Cat order by __key__ desc")
            ref = fetcher.request(mkey, query, num)
            yield ref.get

        fetcher = DataFetcher()
        cats1 = fetcher.spawn_and_run(requester)
        cats2 = fetcher.get(mkey)

        self.assert_obj_list_equals(cats, cats1)
        self.assert_obj_list_equals(cats, cats2)
        self.assert_stats(1, 1, 1)


    # Tests a query function that does multiple rounds of fetching.
    def _test_single_multi_step_query(self):
        cat, friend = pair = self.make_cat_pair()

        def do_fetch():
            requester = partial(self.request_cat_and_friend, cat)
            fetcher = DataFetcher()
            result = fetcher.spawn_and_run(requester)
            return (fetcher, result)
            
        fetcher, result = do_fetch()
        self.check_cat_pair_fetched(pair, result, fetcher)

        self.do_test_cache(do_fetch, 2, 2, 2)

    # Similar to the function above, but tests using multiple functions
    # that make a single request on each pass
    def _test_multiple_multi_step_queries(self):
        def get_requester_funcs(cat_pairs):
            return [partial(self.request_cat_and_friend, pair[0]) for pair in cat_pairs]

        self.do_multiple_multi_step_queries(get_requester_funcs)


    # Tests fetching multiple objects on the first pass and then
    # multiple related objects on the second pass.
    def _test_multiple_multi_step_queries2(self):
        def get_requester_funcs(cat_pairs):

            # Note: For this type of use case
            # (fetching multiple objects in each pass)
            # it would be better to use a requester per each object
            # instead of a single requester for all objects, but for
            # testing we want to ensure this logic works too.
            def requester(fetcher):
                cat_refs = []
                for pair in cat_pairs:
                    id = pair[0].key().id()
                    cat_refs.append(fetcher.request_obj(Cat, id))
                
                yield

                friend_refs = []
                for cat_ref in cat_refs:
                    cat = cat_ref.get()
                    friend_id = cat.friend_id
                    friend_ref = fetcher.request_obj(Cat, friend_id)
                    friend_refs.append(friend_ref)

                def ondone():
                    return [(cat_refs[i].get(), friend_refs[i].get())
                            for i in xrange(len(cat_refs))]

                yield ondone

            return requester

        self.do_multiple_multi_step_queries(get_requester_funcs)

    def do_multiple_multi_step_queries(self, get_requester_funcs):
        cat_pairs = [self.make_cat_pair() for i in range(3)]

        def do_fetch():
            requester_funcs = get_requester_funcs(cat_pairs)
            fetcher = DataFetcher()
            if type(requester_funcs) is list:
                results = fetcher.spawn_and_run_multi(requester_funcs)
            else:
                results = fetcher.spawn_and_run(requester_funcs)
            return (fetcher, results)

        (fetcher, results) = do_fetch()
        for i in xrange(len(cat_pairs)):
            pair = cat_pairs[i]
            self.check_cat_pair_fetched(pair, results[i], fetcher)

        self.do_test_cache(do_fetch, 2, 6, 2)

    # A helper function for testing the use of the local cache as well as memcache
    # when fetching data.
    def do_test_cache(self, do_fetch, num_fetches, num_requests, num_db_fetches):
        # On the first fetch, all requests should have been fetched from 
        # the data store.
        self.assert_stats(num_fetches, num_requests, num_db_fetches)

        # On the second fetch of the same data, the listener shouldn't
        # have any stats incremented as the data should have been fetched
        # from local cache.
        do_fetch()
        self.assert_stats(num_fetches, num_requests, num_db_fetches)

        # If we clear the local cache and request the same keys,
        # the data should be fetched from memcache.
        DataFetcher.reset()
        DataFetcher.set_listener(DataFetcherTestListener())
        do_fetch()
        self.assert_stats(num_fetches, num_requests, 0)

    # Tests that spawn() behaves as expected when called from
    # from within a requester function.
    def _test_inner_spawn(self):
        cat, friend = pair = self.make_cat_pair()

        def requester(fetcher):
            fetch_inner = partial(self.request_cat_and_friend, cat)
            fetcher.spawn(fetch_inner)

            yield

            cat1 = fetcher.get_obj(Cat, cat.key().id())
            self.assert_cat_equals(cat, cat1)
            
            # Verify that after the first pass, we shouldn't have gotten the
            # friend's data
            self.assertRaises(KeyError, fetcher.get_obj, Cat, friend.key().id())

            def ondone():
                # after the second pass, we should have the friend's data
                friend1 = fetcher.get_obj(Cat, friend.key().id())
                self.assert_cat_equals(friend, friend1)
                return (cat1, friend1)
                
            yield ondone

        fetcher = DataFetcher()
        result = fetcher.spawn_and_run(requester)
        self.check_cat_pair_fetched(pair, result, fetcher)
        self.assert_stats(2, 2, 2)

    # Tests the use of spawn_and_join() from within a requester function.
    def _test_spawn_and_join(self):
        pair = self.make_cat_pair()
        requester = partial(self.requester_with_join, pair)
        fetcher = DataFetcher()
        result = fetcher.spawn_and_run(requester)
        self.check_cat_pair_fetched(pair, result, fetcher)
        self.assert_stats(2, 2, 2)


    # Tests that even when two parallel requesters request the same
    # data on the second pass, the data is fetched only once.
    def _test_multi_spawn_and_join_with_common_friend(self):
        cat1 = self.make_cat()
        cat2 = self.make_cat()
        friend = self.make_cat()

        cat1.friend_id = friend.key().id()
        cat2.friend_id = friend.key().id()
        db.put([cat1, cat2])
        
        pairs = [(cat1, friend), (cat2, friend)]
        requesters = [partial(self.requester_with_join, pair) for pair in pairs]

        fetcher = DataFetcher()
        results = fetcher.spawn_and_run_multi(requesters)

        for i in xrange(len(pairs)):
            self.check_cat_pair_fetched(pairs[i], results[i], fetcher)

        self.assert_stats(2, 3, 2)

    def test_spawn_with_ondone_func(self):
        pair = cat, friend = self.make_cat_pair()

        def requester(fetcher):
            requester1 = partial(self.request_cat_and_friend, cat)
            ref = fetcher.spawn(requester1)
            yield ref.get

        fetcher = DataFetcher()
        res = fetcher.spawn_and_run(requester)
        self.check_cat_pair_fetched(pair, res, fetcher)


    def _test_fetch_obj(self):
        cat, friend = pair = self.make_cat_pair()
        cat1 = DataFetcher.fetch_obj(Cat, cat.key().id())
        friend1 = DataFetcher.fetch_obj(Cat, friend.key().id())
        self.assert_obj_list_equals(pair, [cat1, friend1])

    def _test_fetch_objs(self):
        pair = self.make_cat_pair()
        pair1 = DataFetcher.fetch_objs(Cat, [cat.key().id() for cat in pair])
        self.assert_obj_list_equals(pair, pair1)
        

    # Tests that the get*() functions can't be called before run().
    def _test_not_run_exception(self):
        fetcher = DataFetcher()
        self.assertRaises(NotRunException, fetcher.get, "foo")
        self.assertRaises(NotRunException, fetcher.get_obj, Cat, 1)

    # Test that run() can't be called twice.
    def _test_already_ran_exception(self):
        fetcher = DataFetcher().run()
        self.assertRaises(AlreadyRanException, fetcher.run)

        def requester(fetcher):
            fetcher.run()

        fetcher = DataFetcher()
        fetcher.spawn(requester)
        self.assertRaises(AlreadyRanException, fetcher.run)

    # Test that the request*() functions can only be called when
    # the fetcher is running
    def _test_not_running_exception(self):
        fetcher = DataFetcher()
        self.assertRaises(NotRunningException,
                          fetcher.request,
                          "some_key",
                          db.GqlQuery("select * from Cat"))

        self.assertRaises(NotRunningException,
                          fetcher.request_obj,
                          Cat,
                          1)

    # Tests that an exception is thrown if you attempt to call
    # 'yield' after a call to 'yield callable'
    def _test_too_many_yields_exception(self):
        def requester(self, fetcher):
            yield lambda: "foo"
            yield
        fetcher = DataFetcher()
        self.assertRaises(Exception, fetcher.spawn_and_run, requester)

    # A helper function for requesting a single cat using Gql.
    @staticmethod
    def request_cat(cat, fetcher):
        query = db.GqlQuery("select * from Cat where __key__=:1", cat.key())
        ref = fetcher.request(cat.get_mkey(), query)
        yield ref.get
        
    # A helper function for requesting objects from a fetcher
    # in two passes.
    @staticmethod
    def request_cat_and_friend(cat, fetcher):
        id = cat.key().id()
        cat_ref = fetcher.request_obj(Cat, id)
        yield
        cat = cat_ref.get()
        friend_id = cat.friend_id
        friend_ref = fetcher.request_obj(Cat, friend_id)
        yield lambda: (cat, friend_ref.get())


    # A requester function that fetchers the cat and his friend in
    # a nested call to spawn_and_join().
    def requester_with_join(self, pair, fetcher):
        cat, friend = pair
        fetch_inner = partial(self.request_cat_and_friend, cat)
        ref = fetcher.spawn_and_join(fetch_inner)

        def ondone():
            cat1 = fetcher.get_obj(Cat, cat.key().id())
            friend1 = fetcher.get_obj(Cat, friend.key().id())
            self.assert_obj_list_equals(pair, [cat1, friend1])

            pair1 = ref.get()
            self.assert_obj_list_equals(pair, pair1)

            return pair1

        yield ondone


    def assert_cat_equals(self, cat1, cat2):
        self.assertEquals(cat1.key().id(), cat2.key().id())
        self.assertEquals(cat1.name, cat2.name)

    @classmethod
    def make_cat_pair(cls):
        pair = (cls.make_cat(), cls.make_cat())
        pair[0].friend_id = pair[1].key().id()
        pair[0].put()
        return pair

    # Checks that the cat pair has been fetched properly
    def check_cat_pair_fetched(self, pair, result_from_ref, fetcher):
        self.assert_obj_list_equals(pair, result_from_ref)

        id1 = pair[0].key().id()
        cat = fetcher.get_obj(Cat, id1)
        friend = fetcher.get_obj(Cat, cat.friend_id)
        self.assert_obj_list_equals(pair, [cat, friend])

    @classmethod
    def make_cat(cls):
        name = "Felix" + str(random.random())
        cat = Cat(name=name)
        cat.put()
        return cat
