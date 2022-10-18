import time
import datetime
from threading import Thread
from queue import Queue
import os
from requests import post
import sys
from clickhouse_api import ClickhouseRow, string_from_struct, \
    get_clickhouse_url_generator, get_query_generator_from_string


class ArgumentNotFoundError(Exception):
    ...


try:
    if len(sys.argv) == 2 and sys.argv[1].startswith('--listen-file='):
        FILE = sys.argv[1].split('=')[1]
        os.path.getsize(FILE)
    else:
        raise ArgumentNotFoundError
except FileNotFoundError:
    raise FileNotFoundError(
        f"File not found: {sys.argv[1].split('=')[1]}")
except ArgumentNotFoundError:
    raise ArgumentNotFoundError('You need to set argument '
                                '--listen-file=<filepath> '
                                'for listening this file')

MAX_WRITE_TIME_GAP_CH = 3
MAX_WRITE_COUNT_CH = 1000
MAX_READ_SIZE_LOG = 1024

files = Queue()
logs = Queue()
structured_logs = Queue()
clickhouse_prepared_data_strings = Queue()


def write_to_clickhouse():
    url_generator = get_clickhouse_url_generator()
    query_generator = get_query_generator_from_string()
    for data in iter(clickhouse_prepared_data_strings.get, None):
        post(url_generator(query_generator(data)[0]))


def write_in_clickhouse_queue():
    strings = []
    count = 0
    last_write_time = time.time()
    for log in iter(structured_logs.get, None):
        if not log:
            continue
        strings.append(string_from_struct(log))
        count += 1
        time_over = (new_last_time := time.time()
                     ) - last_write_time > MAX_WRITE_TIME_GAP_CH
        count_over = count > MAX_WRITE_COUNT_CH
        if time_over or count_over:
            clickhouse_prepared_data_strings.put(', '.join(strings))
            strings = []
            count = 0
            last_write_time = new_last_time


def log_line_to_struct(log_line: str) -> ClickhouseRow:
    splitted = log_line.split(' - ')
    inner_ip, outer_ip, timestamp, guid = splitted[:4]
    timestamp = datetime.datetime.strptime(
        timestamp.split(' ')[0].replace(':', ' ', 1),
        '%d/%b/%Y %H:%M:%S'
    ).strftime('%Y-%m-%d %H:%M:%S')
    return ClickhouseRow(
        Timestamp=timestamp,
        GUID=guid,
        OuterIP=outer_ip,
        NgToken='',
        OpenVPNServer='127.0.0.1',
        InnerIP=inner_ip
    )


def on_modified():
    for file in iter(files.get, None):
        with open(file['file'], 'rb') as f:
            list(map(lambda x:
                     structured_logs.put(log_line_to_struct(x)),
                     filter(lambda y: len(y.split(' - ')) > 3,
                            f.read()[file['start_from']:]
                            .decode('utf-8').strip('\n').split('\n'))))


def check_modified():
    file_size = os.path.getsize(FILE)
    last_time = time.time()
    while True:
        new_size = os.path.getsize(FILE)
        max_time_over = (new_time := time.time()
                         ) - last_time > MAX_WRITE_TIME_GAP_CH
        file_changed = new_size != file_size
        max_file_changed = new_size - file_size > MAX_READ_SIZE_LOG
        if (max_time_over and file_changed) or max_file_changed:
            files.put({'file': FILE, 'start_from': file_size})
            file_size = new_size
            last_time = new_time
        time.sleep(0.2)


def configure_watchdog():
    check_thread = Thread(target=check_modified)
    on_modified_thread = Thread(target=on_modified)
    write_in_ch_queue_thread = Thread(target=write_in_clickhouse_queue)
    write_to_ch_thread = Thread(target=write_to_clickhouse)

    check_thread.start()
    on_modified_thread.start()
    write_in_ch_queue_thread.start()
    write_to_ch_thread.start()


def main():
    configure_watchdog()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        ...
