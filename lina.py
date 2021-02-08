import sys
import argparse
import time
import re
import queue
import urllib.request
import concurrent.futures as cuf
import threading
import sqlite3
from http.server import BaseHTTPRequestHandler as HTTP


REQUEST_TIMEOUT = 5
GET2SUBMIT_TIMEOUT = 10


q = queue.Queue()
link_num = 0
db_mutex = threading.Lock()
show_mutex = threading.Lock()
fe_mutex = threading.Lock()


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


head_suffix = re.compile(
    r'[.](jpg|jpeg|png|gif|webp|css|js|txt|xml|gzip|rar|7z)$')


# This function is running concurrently!
def check_url(url, start_url, single, dbfile, relaxtime, exclude):
    # Unknown error had been observed, and thread will terminated silently.
    # So here use another try except structure to catch any uncatched error.
    try:
        #
        with db_mutex:
            try:
                conn = sqlite3.connect(dbfile)
                r = conn.execute('SELECT status FROM link_data WHERE link=?',
                                 (url,))
                if r.fetchone() is None:
                    # determine url type
                    url_type = 0  # normal html page
                    if head_suffix.search(url.lower()):
                        url_type = 1  # resource page
                    conn.execute('INSERT INTO link_data VALUES (?,?,?,?,?)',
                                 (None,url,url_type,-1,None))
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
            if url_type == 1:
                status = http_head(url, timeout=REQUEST_TIMEOUT)
                bcont = None
            else:
                status, bcont = http_get(url, timeout=REQUEST_TIMEOUT)
        except Exception as e:
            status = str(e)
            bcont = None
        finally:
            with db_mutex:
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
            with show_mutex:
                if status == 200:
                    print(url, status, 'OK')
                else:
                    print(url, end=' ')
                    cprint(status, end=' ', fg='m')
                    try:
                        cprint(HTTP.responses[status], end=' ', fg='m')
                    except KeyError:
                        ...
                    finally:
                        print()
                cprint(' Workers:%d, Links:%d'
                       % (threading.active_count()-1,link_num),
                       end='\r',
                       flush=True,
                       fg='g', style='inverse')
        # only links with same domain prefix need to parse
        if status==200 and bcont and re.match(start_url, url):
            parse_flag = True
            if single:
                if url != start_url:
                    parse_flag = False
            if parse_flag:
                bcont = bcont.decode()
                # here we decide what kind of links to process
                # 1. href="link"
                urlset1 = set(re.findall(r'href="(http.*?[^#]*?)"', bcont))
                # 2. src="link"
                urlset2 = set(re.findall(r'img.*?src="(http.*?)"', bcont))
                # merge to one set and remove excludes
                urlset = urlset1 | urlset2
                for it in list(urlset):
                    if exclude:
                        if exclude.search(it):
                            urlset.remove(it)
                with db_mutex:
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
                # put sub links which are not checked to queue
                with db_mutex:
                    try:
                        conn = sqlite3.connect(dbfile)
                        for it in map(str.strip, urlset):
                            r = conn.execute(
                                'SELECT link FROM link_data WHERE link=?',
                                (it,))
                            if r.fetchone() is None:
                                q.put(it)
                    except Exception as e:
                        print('Exception 4:', repr(e))
                    finally:
                        conn.close()
    except Exception as e:
        with fe_mutex:
            with open('error.txt', 'a') as f:
                f.write(repr(e))
                f.write('\n')
    finally:
        if relaxtime: time.sleep(relaxtime)


def main():
    parser = argparse.ArgumentParser()
    actType = parser.add_mutually_exclusive_group(required=True)
    actType.add_argument('--url',  help='start url with http[s] prefix')
    actType.add_argument('--stat', action='store_true',
                         help='show stat for a database')
    parser.add_argument('-d', '--database', required=True,
                        help='name a sqlite database to store data')
    parser.add_argument('-s', '--single', action='store_true',
                        help='single page mode')
    parser.add_argument('-w', '--worker', type=int,
                        help='how many worker thread')
    parser.add_argument(
        '-t', '--relaxtime', type=int,
        help='relax N microseconds just before return of each worker')
    parser.add_argument('-e', '--exclude',
                        help='exclude urls which hit the re pattern')
    args = parser.parse_args()
    if not args.stat:
        # init database
        init_sql = """
        BEGIN EXCLUSIVE;
        CREATE TABLE IF NOT EXISTS link_data (
            link_id INTEGER PRIMARY KEY,
            link TEXT UNIQUE,
            type INT,
            status TEXT,
            sub_links TEXT);
        COMMIT;
        """
        conn = sqlite3.connect(args.database)
        conn.executescript(init_sql)
        conn.close()
        # put incompleted urls in queue
        qlist = []
        qlist.append(args.url.strip())
        conn = sqlite3.connect(args.database)
        # (1) link status is not 200
        r = conn.execute('SELECT link FROM link_data WHERE status!=200')
        for row in r.fetchall():
            qlist.append(row[0])
        r = conn.execute('DELETE FROM link_data WHERE status!=200')
        conn.commit()
        # (2) sub_links is null
        r = conn.execute(
            'SELECT link FROM link_data WHERE type==0 and sub_links is null')
        for row in r.fetchall():
            qlist.append(row[0])
        r = conn.execute(
            'DELETE FROM link_data WHERE type==0 and sub_links is null')
        conn.commit()
        # (3) links in sub_links which are not stored
        r = conn.execute(
            'SELECT sub_links FROM link_data WHERE sub_links is not null')
        for row in r.fetchall():
            for link in eval(row[0]):
                link = link.strip()
                s = conn.execute('SELECT status FROM link_data WHERE link=?',
                                 (link,))
                if s.fetchone() is None:
                    qlist.append(link)
        conn.close()
        for it in set(qlist):
            q.put(it)
            print('# add %s' % it)
        # create thread pool
        tpool = (cuf.ThreadPoolExecutor() if args.worker is None
                 else cuf.ThreadPoolExecutor(max_workers=args.worker))
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
                         args.relaxtime/1000,
                         re.compile(args.exclude) if args.exclude else None)
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
