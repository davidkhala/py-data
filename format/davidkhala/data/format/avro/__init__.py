from typing import Iterator

import fastavro


def read(file_path) -> Iterator:
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        for record in reader:
            print(type(record)) # TODO
            yield record
