from typing import Iterator

import fastavro


def read(content) -> Iterator[dict]:
    reader = fastavro.reader(content)
    for record in reader:
        yield record


def is_avro(file_path:str):
    return fastavro.is_avro(file_path)
