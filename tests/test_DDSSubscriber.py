import unittest
import lsst.utils.tests
from lsst.ts.salpytools import salpylib


class TestDDSSubscriberThread(unittest.TestCase):

    def test_invalid_parse_for_subsystem_tag(self):

        # Incorrectly formatted topics
        topic1 = "some_incorrectlyformated_topic"
        topic2 = "someincorrectlyforamtedtopic"
        topic3 = "some_incorrectly_formated_topic"
        topic4 = "_command"
        topic5 = "command_"
        topic6 = "___"
        topic7 = "__"
        topic8 = "_"

        handle = {}

        with self.assertRaises(ValueError):
            subscriber = salpylib.DDSSubscriberThread(topic = topic1, handle = handle)
        with self.assertRaises(ValueError):
           subscriber = salpylib.DDSSubscriberThread(topic = topic2, handle = handle)
        with self.assertRaises(ValueError):
            subscriber = salpylib.DDSSubscriberThread(topic = topic3, handle = handle)
        with self.assertRaises(ValueError):
            subscriber = salpylib.DDSSubscriberThread(topic = topic4, handle = handle)
        with self.assertRaises(ValueError):
            subscriber = salpylib.DDSSubscriberThread(topic = topic5, handle = handle)
        with self.assertRaises(ValueError):
            subscriber = salpylib.DDSSubscriberThread(topic = topic6, handle = handle)
        with self.assertRaises(ValueError):
            subscriber = salpylib.DDSSubscriberThread(topic = topic7, handle = handle)
        with self.assertRaises(ValueError):
            subscriber = salpylib.DDSSubscriberThread(topic = topic8, handle = handle)

    def test_valid_parse_for_subsystem_tag(self):

        # Correctly formatted topics
        topic1 = "subsystemtag_command_arbitrarystring"
        topic2 = "subsystemtag_logevent_arbitrarystring"
        topic3 = "subsystemtag_arbitrarystring"

        handle = {}

        subscriber1 = salpylib.DDSSubscriberThread(topic = topic1, handle = handle)

        subscriber2 = salpylib.DDSSubscriberThread(topic = topic2, handle = handle)

        subscriber3 = salpylib.DDSSubscriberThread(topic = topic3, handle = handle)

        self.assertEqual(subscriber1.subsystem_tag, "subsystemtag")
        self.assertEqual(subscriber2.subsystem_tag, "subsystemtag")
        self.assertEqual(subscriber3.subsystem_tag, "subsystemtag")

    def test_invalid_SALPY_import(self):
        # Invalid subsytem tag passed to the DDSSubscirber
        topic = "invalid_subsytemtag"
        handle = {}
        subscriber = salpylib.DDSSubscriberThread(topic = topic, handle = handle)
        with self.assertRaises(ModuleNotFoundError):
            subscriber.set_salpy_lib()

    def test_valid_SALPY_import(self):
        # Simple instantiation of a DDSSubscriberThread
        topic = "scheduler_notneededforthistest"
        handle = {}
        subscriber = salpylib.DDSSubscriberThread(topic = topic, handle = handle)
        subscriber.set_salpy_lib()
        self.assertEqual(subscriber.subsystem_tag, "scheduler")

    def test_command_topic(self):
        # The DDSSubsciber should not handle commands
        topic = "scheduler_command_enterControl"
        handle = {}
        subscriber = salpylib.DDSSubscriberThread(topic = topic, handle = handle)
        subscriber.set_salpy_lib()
        subscriber.set_mgr()
        with self.assertRaises(ValueError):
            subscriber.mgr_subscribe_to_topic()

    def test_event_topic(self):
        # Only the flag for the event should be true, the others should be false.
        topic = "scheduler_logevent_target"
        handle = {}
        subscriber = salpylib.DDSSubscriberThread(topic = topic, handle = handle)
        subscriber.set_salpy_lib()
        subscriber.set_mgr()
        subscriber.mgr_subscribe_to_topic()

        self.assertTrue(subscriber.is_event)
        self.assertFalse(subscriber.is_command)
        self.assertFalse(subscriber.is_telemetry)

    def test_telemetry_topic(self):
        # Only the flag for the telemetry should be true, the others should be false.
        topic = "scheduler_bulkCloud"
        handle = {}
        subscriber = salpylib.DDSSubscriberThread(topic = topic, handle = handle)
        subscriber.set_salpy_lib()
        subscriber.set_mgr()
        subscriber.mgr_subscribe_to_topic()

        self.assertTrue(subscriber.is_telemetry)
        self.assertFalse(subscriber.is_command)
        self.assertFalse(subscriber.is_event)

    def test_invalid_set_data(self):
        # Only the flag for the telemetry should be true, the others should be false.
        topic = "scheduler_bulkCloud"
        handle = {}
        subscriber = salpylib.DDSSubscriberThread(topic = topic, handle = handle)
        subscriber.set_salpy_lib()
        subscriber.set_mgr()
        # Invalid because we should call subscriber.mgr_subscribe_to_topic()
        with self.assertRaises(ValueError):
            subscriber.set_data()

    def test_valid_set_data(self):
        # Only the flag for the telemetry should be true, the others should be false.
        topic = "scheduler_bulkCloud"
        handle = {}
        subscriber = salpylib.DDSSubscriberThread(topic = topic, handle = handle)
        subscriber.set_salpy_lib()
        subscriber.set_mgr()
        subscriber.mgr_subscribe_to_topic()


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
