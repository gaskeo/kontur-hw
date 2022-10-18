from pandas import read_csv
import sys
import os
from typing import List
from clickhouse_api import ClickhouseRow, insert_into_clickhouse


class ArgumentNotFoundError(Exception):
    ...


try:
    if len(sys.argv) == 2 and sys.argv[1].startswith('--sample-file='):
        FILE = sys.argv[1].split('=')[1]
        os.path.getsize(FILE)
    else:
        raise ArgumentNotFoundError
except FileNotFoundError:
    raise FileNotFoundError(
        f"File not found: {sys.argv[1].split('=')[1]}")
except ArgumentNotFoundError:
    raise ArgumentNotFoundError('You need to set argument '
                                '--sample-file=<filepath> '
                                'for listening this file')


def prepare_data(raw_data: List[List[str]]) -> List[ClickhouseRow]:
    prepared_data = list(map(lambda row: ClickhouseRow(
        Timestamp=row[0],
        GUID=row[1],
        OuterIP=row[2],
        NgToken=row[3],
        OpenVPNServer=row[4],
        InnerIP=row[5]
    ), raw_data))
    return prepared_data


def main():
    file = read_csv(FILE)
    prepared_data = prepare_data(
        file.drop(axis=1, labels=['Unnamed: 0']).values.tolist())
    insert_into_clickhouse(prepared_data)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        ...
