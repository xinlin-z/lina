import sys
import argparse
import time
import re
import queue
import urllib.request
import concurrent.futures as cuf
import threading
import sqlite3
import http.server


REQUEST_TIMEOUT = 5
GET2SUBMIT_TIMEOUT = 10


q = queue.Queue()
link_num = 0
mutex = threading.Lock()


def cprint(*objects, sep=' ', end='\n', file=sys.stdout,
           flush=False, fg=None, bg=None, style='default'):
    """colorful print.
    Color and style the string and background, then call the print function,
    Eg: cprint('pynote.net', fg='red', bg='green', style='blink')
    The other parameters are the same with stand print!
    """
    def _ct(code='0'):
        return '\033[%sm'%code

    # text color
    c = 37
    if fg in ('red','r'): c = 31
    elif fg in ('green','g'): c = 32
    elif fg in ('yellow','y'): c = 33
    elif fg in ('blue','b'): c = 34
    elif fg in ('magenta','m'): c = 35
    elif fg in ('cyan','c'): c = 36
    elif fg in ('white','w'): c = 37
    elif fg in ('black','k'): c = 30
    # background color
    b = 40
    if bg in ('red','r'): b = 41
    elif bg in ('green','g'): b = 42
    elif bg in ('yellow','y'): b = 43
    elif bg in ('blue','b'): b = 44
    elif bg in ('magenta','m'): b = 45
    elif bg in ('cyan','c'): b = 46
    elif bg in ('white','w'): b = 47
    elif bg in ('black','k'): b = 40
    # style
    a = 0
    if style == 'underline': a = 4
    elif style == 'blink': a = 5
    elif style == 'inverse': a = 7

    string = sep.join(map(str, objects))
    color = '%d;%d;%d' % (a,c,b)
    print(_ct(color)+string+_ct(), sep=sep, end=end, file=file, flush=flush)


def http_get(url, ua=None, timeout=3):
    """HTTP GET method for an url, return (status,content) or raise."""
    try:
        req = (urllib.request.Request(url,headers={'User-Agent':ua}) if ua
               else urllib.request.Request(url))
        with urllib.request.urlopen(req,timeout=timeout) as res:
            return res.status, res.read()
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        raise


def http_head(url, ua=None, timeout=3):
    """HTTP HEAD method for an url, return status code or raise."""
    try:
        req = (urllib.request.Request(url,
                                      headers={'User-Agent':ua},
                                      method='HEAD') if ua else
               urllib.request.Request(url,method='HEAD'))
        with urllib.request.urlopen(req,timeout=timeout) as res:
            return res.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        raise


# This function is running concurrently.
def check_url(url, start_url, single, dbfile, relaxtime, exclude):
    # Unknown error had been observed, and thread will terminated silently.
    # So here use another try except structure to catch any uncatched error.
    try:
        # do exclude
        if exclude:
            if re.search(exclude, url):
                print(url, end=' ')
                cprint('skipped', fg='y')
                return
        #
        with mutex:
            try:
                conn = sqlite3.connect(dbfile)
                r = conn.execute('SELECT status FROM link_data WHERE link=?',
                                                                        (url,))
                if r.fetchone() is None:
                    conn.execute('INSERT INTO link_data VALUES (?,?,?,?)',
                                                            (None,url,-1,None))
                    conn.commit()
                else:
                    return
            except Exception as e:
                print('Exception 1:', repr(e))
                return
            finally:
                conn.close()
        #
        try:
            if re.search(r'[.](jpg|jpeg|png|gif|webp|css|js|txt|xml)$',
                         url.lower()):
                status = http_head(url, timeout=REQUEST_TIMEOUT)
                bcont = None
            else:
                status, bcont = http_get(url, timeout=REQUEST_TIMEOUT)
        except Exception as e:
            status = str(e)
            bcont = None
        finally:
            with mutex:
                global link_num
                link_num += 1
                try:
                    conn = sqlite3.connect(dbfile)
                    conn.execute('UPDATE link_data SET status=? where link=?',
                                 (status, url))
                    conn.commit()
                except Exception as e:
                    print('Exception 2:', repr(e))
                    return
                finally:
                    conn.close()
            # show out
            if status == 200:
                print(url, status, 'OK')
            else:
                print(url, end=' ')
                cprint(status, end=' ', fg='m')
                try:
                  cprint(http.server.BaseHTTPRequestHandler.responses[status],
                         end=' ', fg='m')
                except KeyError:
                    ...
                finally:
                    print()
            cprint(' Workers:%d, Links:%d'
                   % (threading.active_count()-1,link_num),end='\r',flush=True,
                   fg='g', style='inverse')
        # only links with same domain prefix need to parse
        if status==200 and bcont and re.match(start_url, url):
            parse_flag = True
            if single:
                if url != start_url: parse_flag = False
            if parse_flag:
                bcont = bcont.decode()
                # here we decide what kind of links to process
                # 1. href="link"
                urlset1 = set(re.findall(r'href="(http.*?[^#]*?)"', bcont))
                # 2. src="link"
                urlset2 = set(re.findall(r'img.*?src="(http.*?)"', bcont))
                # merge to one set
                urlset = urlset1 | urlset2
                with mutex:
                    try:
                        conn = sqlite3.connect(dbfile)
                        conn.execute(
                            'UPDATE link_data SET sub_links=? where link=?',
                            (str(urlset), url))
                        conn.commit()
                    except Exception as e:
                        print('Exception 3:', repr(e))
                    finally:
                        conn.close()
                for it in urlset:
                    q.put(it)
    except Exception as e:
        with mutex:
            with open('error.txt', 'a') as f:
                f.write(repr(e))
                f.write('\n')
    finally:
        if relaxtime: time.sleep(relaxtime)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--url', required=True,
                        help='start url with http[s] prefix')
    parser.add_argument('-d', '--database', required=True,
                        help='name a sqlite database to store data')
    parser.add_argument('-s', '--single', action='store_true',
                        help='single page mode')
    parser.add_argument('-w', '--worker', type=int,
                        help='how many worker thread')
    parser.add_argument(
        '-t', '--relaxtime', type=int,
        help='relax N seconds just before return of each worker')
    parser.add_argument(
        '-e', '--exclude',
        help='exclude urls which hit the re pattern')
    args = parser.parse_args()
    # init database
    init_sql = """
    BEGIN EXCLUSIVE;
    CREATE TABLE IF NOT EXISTS link_data (
        link_id INTEGER PRIMARY KEY,
        link TEXT UNIQUE,
        status TEXT,
        sub_links TEXT);
    COMMIT;
    """
    conn = sqlite3.connect(args.database)
    conn.executescript(init_sql)
    conn.close()
    # create thread pool
    tpool = (cuf.ThreadPoolExecutor() if args.worker is None
             else cuf.ThreadPoolExecutor(max_workers=args.worker))
    # put init urls in queue
    q.put(args.url)
    conn = sqlite3.connect(args.database)
    conn.execute('DELETE FROM link_data WHERE link=?', (args.url,))
    conn.commit()
    r = conn.execute('SELECT link FROM link_data WHERE status!=200')
    for row in r.fetchall():
        conn.execute('DELETE FROM link_data WHERE link=?', (row[0],))
        q.put(row[0])
        print('# add %s to queue from %s' % (row[0], args.database))
    conn.commit()
    conn.close()
    # loop, get from queue and submit
    while True:
        try:
            url = q.get(timeout=GET2SUBMIT_TIMEOUT)
        except queue.Empty:
            # wait all futures to stop and free resources
            tpool.shutdown()
            print('GET2SUBMIT timeout, submit done...')
            break
        tpool.submit(check_url,
                     url,
                     args.url,
                     args.single,
                     args.database,
                     args.relaxtime,
                     args.exclude)
    # stat data in database
    cprint('Stat in database %s:' % args.database, fg='g')
    conn = sqlite3.connect(args.database)
    r = conn.execute('SELECT status,count(status) FROM link_data'
                     ' GROUP BY status')
    print('status code : link number')
    for row in r.fetchall():
        print((str(row[0])+':').ljust(16,' '), row[1])
    conn.close()


if __name__ == '__main__':
    main()
