import unittest

import pickle
import os


class TaskTestCase(unittest.TestCase):
    def test_create_delete_dir(self):
        dirs = set()
        with open(f"test/tasks.txt", 'rb') as f:
            while True:
                try:
                    data = pickle.load(f)

                    if data['target'].__name__ == 'create_dir':
                        dirs.add(f"tmp_{data['kwargs']['uuid']}")
                    if data['target'].__name__ == 'delete_dir':
                        dirs.discard(f"tmp_{data['kwargs']['name_dir']}")
                except EOFError:
                    break
        for el in dirs:
            self.assertEqual(os.path.exists(el), True)

    def test_create_delete_file(self):
        files = set()
        with open(f"test/tasks.txt", 'rb') as f:
            while True:
                try:
                    data = pickle.load(f)

                    if data['target'].__name__ == 'create_file':
                        files.add(f"{data['kwargs']['uuid']}")
                    if data['target'].__name__ == 'delete_file':
                        files.discard(f"{data['kwargs']['name_file']}")
                except EOFError:
                    break
        for el in files:
            self.assertEqual(os.path.exists('tmp/'+el+'.txt'), True)


if __name__ == '__main__':
    unittest.main()
