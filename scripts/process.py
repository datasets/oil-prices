import csv
import urllib
import datautil.tabular.xls as xlstab
import os

data = 'data/'
archive = 'archive/'


def download():
    granularities = [
        ('d', 'day'),
        ('w', 'week'),
        ('m', 'month'),
        ('a', 'year')
    ]
    source = 'https://www.eia.gov/dnav/pet/hist_xls/RBRTE'
    for a in granularities:
        urllib.urlretrieve(
            source + a[0] + '.xls',
            archive + 'brent-' + a[1] + '.xls')

    source = 'http://www.eia.gov/dnav/pet/hist_xls/RWTC'
    for a in granularities:
        urllib.urlretrieve(
            source + a[0] + '.xls',
            archive + 'wti-' + a[1] + '.xls')


def process():
    for dirs, subdirs, files in os.walk(archive):
        for file in files:
            if 'brent' in file:
                header = ['Date', 'Brent Spot Price']
            elif 'wti' in file:
                header = ['Date', 'WTI Spot Price']
            else:
                continue
            reader = xlstab.XlsReader()
            tabdata = reader.read(open(os.path.join(dirs, file)),
                                  sheet_index=1)
            records = tabdata.data
            records = records[3:]
            for x in records:
                if 'year' in file:
                    x[0] = str(x[0].year)
                elif 'month' in file:
                    x[0] = str(x[0].year) + '-' + str(x[0].month).zfill(2)
                else:
                    pass
            name = data + file.split('.xls')[0] + '.csv'
            writer = csv.writer(open(name, 'w'), lineterminator='\n')
            writer.writerow(header)
            writer.writerows(records)


if __name__ == '__main__':
    download()
    process()
