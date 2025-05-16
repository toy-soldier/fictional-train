from unittest import TestCase, mock
from hq_app import main


class TestHQApp(TestCase):

    @mock.patch.object(main.su,"SparkContextManager")
    def test_main(self, mocked_scm):
        ctx = mock.MagicMock()
        mocked_scm().__enter__.return_value = ctx

        main.main()

        # Test whether the context manager was really "called".
        self.assertIn(mock.call("hq_app"), mocked_scm.mock_calls)
        self.assertIn(mock.call().__enter__(), mocked_scm.mock_calls)
        self.assertIn(mock.call().__exit__(None, None, None), mocked_scm.mock_calls)
