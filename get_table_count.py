# Utility Script that was used to watch a running count of tweets being ingested.
import happybase

def get_count(connection_pool):
  with connection_pool.connection() as c:
    c.open()
    t = c.table('tweets')
    count = 0
    for _ in t.scan():
      count += 1
    c.close()
    return count

if __name__ == '__main__':
  cp = happybase.ConnectionPool(
        10,
        host='localhost',
        compat='0.92',
  )
  while True:
    print(get_count(cp), end='\r')

