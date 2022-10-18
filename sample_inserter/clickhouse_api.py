from typing import TypedDict

from typing import NamedTuple, List, Callable
from os import getenv
from urllib.parse import urlencode, urlunparse
from http import HTTPStatus
from requests import get, post, exceptions


class ClickhouseRow(TypedDict):
    Timestamp: str
    GUID: str
    OuterIP: str
    NgToken: str
    OpenVPNServer: str
    InnerIP: str


class UrlComponents(NamedTuple):
    scheme: str
    netloc: str
    url: str
    path: str
    query: str
    fragment: str


class IPAddressCount(NamedTuple):
    ip: str
    count: int


def get_ip_addresses(ch_url_generator: Callable[[str], str]):
    query = '''
    SELECT IP24, sum(cnt) as total_cnt from 
        (select toStartOfHour(Timestamp) as Time, 
        concat(IPv4NumToString(tupleElement(IPv4CIDRToRange(
        IPv4StringToNum(InnerIP), 24), 1)), '/24') as IP24, 
        count() as cnt 
        FROM logs 
        WHERE toStartOfHour(Timestamp) > now() - 60*60*12
        GROUP BY (Time, IP24) HAVING cnt > 20) 
    GROUP BY IP24 ORDER BY total_cnt desc
    '''

    while True:
        try:
            response = get(ch_url_generator(query))
            break
        except exceptions.ConnectionError:
            ...

    if response.status_code == HTTPStatus.OK:
        return process_ip_addresses(response.content)
    return []


def process_ip_addresses(response_bytes: bytes) -> List[IPAddressCount]:
    def str_to_struct(row_string: str):
        ip, count = row_string.split('\t')
        return IPAddressCount(ip, int(count))

    response = response_bytes.decode('utf-8')
    rows = response.strip('\n').split('\n')
    if rows and rows[0] != '':
        return list(map(str_to_struct, rows))
    return []


def get_clickhouse_url_generator():
    def generate_url(query: str):
        params = {
            'user': db_user,
            'password': db_password,
            "database": db_name,
            'query': query
        }

        url = urlunparse(
            UrlComponents(
                scheme='http',
                netloc=f'{db_address}:{db_port}',
                query=urlencode(params),
                path='',
                url='/',
                fragment='',
            )
        )
        return url

    db_name = getenv('DB_NAME')
    db_user = getenv('DB_USER')
    db_password = getenv('DB_PASSWORD')
    db_port = getenv('DB_PORT')
    db_address = getenv('DB_ADDRESS')

    return generate_url


def string_from_struct(struct: ClickhouseRow) -> str:
    return f"('{struct['Timestamp']}', '{struct['GUID']}', " \
           f"'{struct['OuterIP']}', '{struct['NgToken']}', " \
           f"'{struct['OpenVPNServer']}', '{struct['InnerIP']}')"


def get_query_generator_from_list():
    def generate_query(rows: List[ClickhouseRow]):
        batch = list(map(string_from_struct, rows))
        query = 'INSERT INTO logs ' \
                '(Timestamp, GUID, OuterIP, ' \
                'NgToken, OpenVPNServer, InnerIP)' \
                f" VALUES {', '.join(batch)}"
        return query, len(batch)

    return generate_query


def get_query_generator_from_string():
    def generate_query(rows: str):
        query = 'INSERT INTO logs ' \
                '(Timestamp, GUID, OuterIP, ' \
                'NgToken, OpenVPNServer, InnerIP)' \
                f" VALUES {rows}"
        return query, 0

    return generate_query


def insert_into_clickhouse(data: List[ClickhouseRow], batch_size=5_000):
    url_generator = get_clickhouse_url_generator()
    query_generator = get_query_generator_from_list()

    start_index = 0
    written = 0
    while start_index < len(data):
        query, total_batch_size = query_generator(
            data[start_index: start_index + batch_size])

        start_index += batch_size
        written += total_batch_size

        while True:
            try:
                r = post(url_generator(query))
                break
            except exceptions.ConnectionError:
                ...

        if r.status_code == 200:
            print(r.status_code, f'written: {written} rows')
        else:
            print(r.content)


