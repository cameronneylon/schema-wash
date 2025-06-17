import unittest

from schemawash import lib, filter_records

class TestFilters(unittest.TestCase):

    def test_multi_filter(self):
        t1 = filter_records.filter_single_record(
            obj=dict(key1='text',
                     key2=1.0,
                     key3=['a', 'b', 'c']),
            path='key1',
            value=['text2', 'text3']         
        )
        t2 = filter_records.filter_single_record(
            obj=dict(key1='text',
                     key2=1.0,
                     key3=['a', 'b', 'c']),
            path='key1',
            value=['text2', 'text'] 
        )
        assert not t1   
        assert t2      

    def test_multi_filter_desired_false(self):
        t1 = filter_records.filter_single_record(
            obj=dict(key1='text',
                     key2=1.0,
                     key3=['a', 'b', 'c']),
            path='key1',
            value=['text2', 'text3'],
            desired_test_result=False
        )
        t2 = filter_records.filter_single_record(
            obj=dict(key1='text',
                     key2=1.0,
                     key3=['a', 'b', 'c']),
            path='key1',
            value=['text2', 'text'],
            desired_test_result=False
        )
        print(t1)   
        assert t1

        assert not t2     


if __name__ == '__main__':
    unittest.main()