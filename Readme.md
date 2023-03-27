PostgreSQL session storage for [Telethon](https://github.com/LonamiWebs/Telethon).

**Module development in progress**

Usage:

**First of all, create schema in your database.**

```python
from telethon import TelegramClient
from postgres import PostgresqlSession


session = PostgresqlSession(
	dbname="database name",
	schema="created schema name",
	username="user",
	password="password",
)

api_id = 12345
api_hash = "hash"

client = TelegramClient(session, api_id, api_hash)

print(client.get_me().stringify())
```

