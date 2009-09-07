from __future__ import with_statement
import sys
import unittest


class TestAAPickle(unittest.TestCase):

    def test_no_cpickle(self):
        from celery.tests.utils import mask_modules
        prev = sys.modules.pop("flower.serialization")
        with mask_modules("cPickle"):
            from flower.serialization import pickle
            import pickle as orig_pickle
            self.assertTrue(pickle.dumps is orig_pickle.dumps)
        sys.modules["flower.serialization"] = prev
