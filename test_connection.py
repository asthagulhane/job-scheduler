import os
from urllib.parse import urlparse
import pg8000.native
from dotenv import load_dotenv

load_dotenv()
print("DEBUG:", os.getenv("DATABASE_URL"))
url = urlparse(os.getenv("DATABASE_URL"))

conn = pg8000.native.Connection(
    user=url.username,
    password=url.password,
    host=url.hostname,
    port=url.port or 5432,
    database=url.path.lstrip("/"),
    ssl_context=True
)

result = conn.run("SELECT 1;")
print("Connection successful! Result:", result)

conn.close()