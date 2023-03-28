from unittest import TestCase
from unittest.mock import patch, ANY

from gobeventconsumer.__main__ import main


class TestMain(TestCase):

    @patch("gobeventconsumer.__main__.GOBEventConsumer")
    def test_main(self, mock_consumer):
        main()
        mock_consumer.assert_called_with(ANY, ["gebieden", "nap"])
        mock_consumer.return_value.consume.assert_called_once()
