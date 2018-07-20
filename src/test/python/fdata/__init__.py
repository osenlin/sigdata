import unittest



class a(unittest.TestCase):
    def test_case1(self):
        self.assertEqual(1,1,msg="hello")

if __name__=='__main__':
    unittest.main()