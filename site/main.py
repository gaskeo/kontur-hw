from flask import Flask, render_template
from clickhouse_api import get_clickhouse_url_generator, \
    get_ip_addresses

app = Flask(__name__)


@app.route('/')
def index():
    blocked = get_ip_addresses(get_clickhouse_url_generator())
    return render_template('index.html', blocked=blocked)


if __name__ == '__main__':
    app.run('localhost', port=5001)
