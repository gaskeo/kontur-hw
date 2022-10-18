# Домашнее задание


### Общие настройки

1. Развернуть [docker-образ clickhouse](https://hub.docker.com/repository/docker/tikovka72/clickhouse-homework)
Можно сделать с помощью `docker compose` ([пример файла с приложением](docker-compose.yml))
   
2. * [Для блокировщика](#Блокировщик)
   * [Для csv парсера](#CSV-парсер)
   * [Для сайта](#сайт)

## [Блокировщик](/blocker)

Блокирует `IP/24` пользователей, 
которые совершили больше 20 запросов к серверу nginx. 

### Настройка 
   
2. Установить переменные окружения такие же, как и в `docker-compose.yml`:
   ```
   DB_NAME=CLICKHOUSE_DB              
   DB_USER=CLICKHOUSE_USER
   DB_PASSWORD=CLICKHOUSE_PASSWORD
   DB_PORT=<port>
   DB_ADDRESS=localhost
   ```
   
3. Настроить `ipset` и `iptables`: 
   ```
   ipset -N blacklist hash iphash
   iptables -v -I INPUT -m set --match-set blacklist src -j DROP
   ```

4. Настроить логи Nginx, они должны иметь следующий вид:
   ```
   $remote_addr - 
   $proxy_add_x_forwarded_for - 
   $time_local - 
   <custom GUID> - 
   ...'
   ```
   
   За `GUID` можно взять `$pid.$msec.$remote_addr.$connection.$connection_requests`, 
   после `GUID` может идти любая дополнительная информация
   
5. Запустить [`listen_files.py`](/blocker/listen_files.py): 
   ```
   python3 listen_files.py --listen-file=<path/to/file.log>
   ```
   
6. Запустить [`block_ips.py`](/blocker/block_ips.py): 
   ```
   python3 block_ips.py
   ```
   
## CSV парсер 

Вставляет данные с csv файла в `ClickHouse`

Файл должен содержать следующие столбцы:
* Unnamed: 0
* Timestamp
* GUID
* OuterIP
* NgToken
* OpenVPNServer
* InnerIp

### Настройки

2. Установить переменные окружения такие же, как и в `docker-compose.yml`:
   ```
   DB_NAME=CLICKHOUSE_DB              
   DB_USER=CLICKHOUSE_USER
   DB_PASSWORD=CLICKHOUSE_PASSWORD
   DB_PORT=<port>
   DB_ADDRESS=localhost
   ```

3. Запустить [`set_sample.py`](sample_inserter/set_sample.py): 
   ```
   python3 set_sample.py --sample-file=<path/to/sample.csv>
   ```

## Сайт

Простой сайт-пример, на котором отображаются заблокированные `IP/24`

### Настройки

2. Развернуть [docker-образ flask сервера](https://hub.docker.com/repository/docker/tikovka72/flask-homework)
Можно сделать с помощью `docker compose` ([пример файла с clickhouse](docker-compose.yml))
   