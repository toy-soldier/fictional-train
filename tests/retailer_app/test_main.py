"""This module contains the unit test for main.py."""
from unittest import TestCase, mock
from retailer_app import main


class TestRetailerApp(TestCase):

    @mock.patch.object(main.su,"SparkContextManager")
    def test_main(self, mocked_scm):
        ctx = mock.MagicMock()
        mocked_scm().__enter__.return_value = ctx
        ctx.read_files.return_value = mock.Mock(), mock.Mock(), mock.Mock()

        main.main()

        # Test whether the context manager was really "called".
        self.assertIn(mock.call("retailer_app"), mocked_scm.mock_calls)
        self.assertIn(mock.call().__enter__(), mocked_scm.mock_calls)
        self.assertIn(mock.call().__exit__(None, None, None), mocked_scm.mock_calls)
