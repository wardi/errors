#!/usr/bin/env python3

import io
import re
import csv
import sys
import tarfile
import gzip
import lzma
from datetime import datetime

DATE_RE = re.compile(r'^(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d),\d\d\d ')
TB1_RE = re.compile(r'^Traceback \(most recent call last\):$')
FILE1_RE = re.compile(r'^  File ')  # error at end, 1 line extra
# pylons errorhandler output:
URL_RE = re.compile(r'^URL: (.*)$')
FILE2_RE = re.compile(r'^File ')  # error at end, extra lines until:
END_RE = re.compile(r'^------------------------------------------------------------$')

t1 = tarfile.open('uwsgirw1.log.gz.tar')
t2 = tarfile.open('uwsgirw2.log.gz.tar')

w = csv.DictWriter(
    io.TextIOWrapper(gzip.GzipFile('uwsgi.csv.gz', 'w'), encoding="utf-8"),
    fieldnames=['regweb#', 'date', 'url', 'exception', 'traceback', 'extra'],
)
w.writeheader()

for regweb, t in enumerate((t1, t2), 1):
    lastdate = None
    for fn in t.getnames():
        sys.stderr.write(fn+ '\n')
        # wut
        if regweb == 1:
            g = iter(io.TextIOWrapper(lzma.open(t.extractfile(fn)), encoding="utf-8"))
        else:
            g = iter(io.TextIOWrapper(gzip.open(t.extractfile(fn)), encoding="utf-8"))
        row = {}

        def write():
            if not row:
                return
            row['regweb#'] = regweb
            if lastdate:
                row['date'] = lastdate.strftime('%Y-%m-%dT%H:%M:%S')
            w.writerow(row)
            sys.stderr.write('.')
            sys.stderr.flush()
            row.clear()

        try:
            while True:
                line = next(g)
                d = DATE_RE.search(line)
                if d:
                    lastdate = datetime.strptime(d[1], '%Y-%m-%d %H:%M:%S')
                    assert lastdate, dt[0]
                    continue

                tb1 = TB1_RE.search(line)
                if tb1:
                    row['traceback'] = ''
                    while True:
                        line1 = next(g)
                        if FILE1_RE.search(line1):
                            line2 = next(g)
                            row['traceback'] += line1[2:] + line2[2:]
                            continue
                        break
                    row['exception'] = line1.strip()
                    row['extra'] = next(g)
                    write()
                    continue

                # errorhandler output
                url = URL_RE.search(line)
                if not url:
                    continue
                row['url'] = url[1]
                row['traceback'] = ''
                while True:
                    line1 = next(g)
                    if FILE2_RE.search(line1):
                        line2 = next(g)
                        row['traceback'] += line1 + line2
                        continue
                    break
                row['exception'] = line1.strip()
                row['extra'] = ''
                while True:
                    line = next(g)
                    if END_RE.match(line):
                        break
                    row['extra'] += line
                write()

        except StopIteration:
            pass
        write()
        sys.stderr.write('\n')
