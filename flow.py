from clickhouse_driver import Client
import config
client = Client(host=config.host,
                port=config.port,
                user=config.login,
                password=config.password)
client.execute('SELECT now(), version()')
