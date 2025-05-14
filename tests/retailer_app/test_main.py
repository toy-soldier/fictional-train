from unittest import TestCase, mock
from unittest.mock import call
from retailer_app import main


class TestRetailerApp(TestCase):

    @mock.patch.object(main.su,"SparkContextManager")
    def test_main(self, mocked_scm):
        main.main()

        # Test whether the context manager was really "called".
        self.assertIn(call("retailer_app"), mocked_scm.mock_calls)
        self.assertIn(call().__enter__(), mocked_scm.mock_calls)
        self.assertIn(call().__exit__(None, None, None), mocked_scm.mock_calls)
