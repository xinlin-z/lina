import sys
import argparse
import time
import re
import queue
import urllib.request
import concurrent.futures as cuf
import threading
import sqlite3


REQUEST_TIMEOUT = 3
GET2SUBMIT_TIMEOUT = 10


q = queue.Queue()
link_data = {}
link_stat = {}
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


def _print(url, stat):
    if stat == 200:
        print(url, stat, 'OK')
    else:
        print(url, end=' ')
        cprint(stat, fg='m')
    cprint(' Workers:%d, Links:%d'
           % (threading.active_count()-1, len(link_stat)), end='\r',
           fg='g', style='inverse')


def parse_link(url, res):
    html = res.read().decode()
    urlset = set(re.findall(r'href="([^#].*?)"', html))
    link_data[url] = urlset
    for it in urlset:
        with mutex:
            if it in link_stat.keys():
                continue
        q.put(it)


def check_url(url, start_url, single):
    with mutex:
        if url in link_stat.keys():
            return
        else:  # make the key occupied by this thread
            link_stat[url] = None
    try:
        res = urllib.request.urlopen(url, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        link_stat[url] = repr(e)
        _print(url, link_stat[url])
        return
    link_stat[url] = res.status
    _print(url, res.status)
    # only links with same domain need to parse
    if re.match(start_url, url):
        if single:
            if url == start_url:
                parse_link(url, res)
        else:
            parse_link(url, res)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('url', help='start url with http[s] prefix')
    parser.add_argument('-s', '--single', action='store_true',
                        help='single page mode')
    parser.add_argument('-w', '--worker', type=int,
                        help='how many worker thread')
    args = parser.parse_args()
    # create thread pool
    tpool = (cuf.ThreadPoolExecutor() if args.worker is None
             else cuf.ThreadPoolExecutor(max_workers=args.worker))
    # put init url in queue
    q.put(args.url)
    # get from queue and submit
    while True:
        try:
            url = q.get(timeout=GET2SUBMIT_TIMEOUT)
        except queue.Empty:
            # wait all futures to stop and free resources
            tpool.shutdown()
            print('GET2SUBMIT timeout, submit done...')
            break
        tpool.submit(check_url, url, args.url, args.single)
    # save result
    with open(fn:='lina_'+str(time.time())+'.txt', 'w') as f:
        for k,v in link_data.items():
            err_list = []
            for link in v:
                try:
                    if link_stat[link] != 200:
                        err_list.append((link, link_stat[link]))
                except:
                    err_list.append((link, None))
            f.write(f'{k}, {link_stat[k]}\n')
            for it in err_list:
                f.write(f'    {str(it)}\n')
    print('save result to %s done...' % fn)


if __name__ == '__main__':
    main()
