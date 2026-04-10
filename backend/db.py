from psycopg_pool import ConnectionPool
from config import DB_DSN

pool = ConnectionPool(DB_DSN, min_size=3, max_size=10, open=True)
