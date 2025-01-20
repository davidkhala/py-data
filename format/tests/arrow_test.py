import unittest
from pyarrow import input_stream


class Samples(unittest.TestCase):
    def test_input_stream(self):
        #  Create an Arrow input stream and inspect it:
        data = b'reader data'
        buf = memoryview(data)
        with input_stream(buf) as stream:
            print(stream.size())
            print(stream.read(6))
            stream.seek(7)
            print(stream.read(4))


if __name__ == '__main__':
    unittest.main()
