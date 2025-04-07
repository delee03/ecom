import unittest
from unittest.mock import Mock
from airflow.exceptions import AirflowFailException
from include.tasks import _check_size

class TestCheckSize(unittest.TestCase):
    def setUp(self):
        self.mock_ti = Mock()
        
    def test_check_size_raises_exception_for_zero(self):
        self.mock_ti.xcom_pull.return_value = 0
        
        with self.assertRaises(AirflowFailException):
            _check_size(ti=self.mock_ti)
            
        self.mock_ti.xcom_pull.assert_called_once_with(
            key='request_size',
            task_ids='get_cocktail'
        )
    
    def test_check_size_raises_exception_for_negative(self):
        self.mock_ti.xcom_pull.return_value = -1
        
        with self.assertRaises(AirflowFailException):
            _check_size(ti=self.mock_ti)
    
    def test_check_size_passes_for_positive(self):
        self.mock_ti.xcom_pull.return_value = 1
        
        # Should not raise an exception
        _check_size(ti=self.mock_ti)

if __name__ == '__main__':
    unittest.main()