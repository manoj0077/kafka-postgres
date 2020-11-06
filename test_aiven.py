from config import get_config
import consumer
import producer
import psycopg2
import re
import unittest
from unittest import mock


class TestAiven(unittest.TestCase):

    def setUp(self):
        self.config_file_name = "aiven_test_env.ini"

    @mock.patch.object(producer, "success_handler")
    def test_producer(self, handler_check):
        """
        Produces one event in test environment and
        validates whether success handler is triggered or not
        """
        producer.start_producer(1, self.config_file_name)
        self.assertTrue(handler_check.called)

    def test_consumer(self):
        """
        Consumes the event in test environment and if there are any messages
        validates them in the database.
        """
        with self.assertLogs("consumer", level="DEBUG") as f:
            consumer.start_consumer(self.config_file_name)

        output = ''.join(f.output)
        self.assertIn("Messages consumed successfully", output)

        # extract name and phone number from log and validate against database
        number_match = re.match(r".*'phone_number': '(.*?)'.*", output)
        name_match = re.match(r".*'name': '(.*?)'.*", output)
        if number_match and name_match:
            config = get_config(self.config_file_name)
            db_con = None
            try:
                db_con = consumer.get_db_connection(config["POSTGRES"])
                cursor = db_con.cursor()
                select_query = '''
                    select * from persons WHERE PhoneNumber = %s AND Name = %s
                    '''
                cursor.execute(select_query, (number_match.group(1), name_match.group(1)))
                records = cursor.fetchall()
                self.assertEqual(len(records), 1)
            finally:
                if db_con:
                    db_con.close()


if __name__ == '__main__':
    unittest.main()