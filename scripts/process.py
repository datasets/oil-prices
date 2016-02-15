import csv
import urllib
import datautil.tabular.xls as xlstab

date = 'data/'
archive = 'archive/'

def download():
    source = 'http://www.eia.gov/dnav/pet/hist_xls/RWTC'
    for a in ['d', 'w', 'm', 'a']:
        urllib.urlretrieve(source + a + '.xls', archive+'brent-'+a+'.xls')
    '''
    reader = xlstab.XlsReader()
    tabdata = reader.read(open(in_path), sheet_index=0)
    records = tabdata.data
    header = ['Date', 'Cost per Mb', 'Cost per Genome']
    records = records[1:]
    for x in records:
        x[0] = str(x[0].year)+'-'+str(x[0].month)
        x[1] = "%.3f" % float(x[1])
        x[2] = "%.3f" % float(x[2])
    #print (records)

    writer = csv.writer(open(out_path, 'w'), lineterminator='\n')
    writer.writerow(header)
    writer.writerows(records)
'''


def process_brent_crude():



def process_wti():



if __name__ == '__main__':
    download()
    process_brent_crude()
    process_wti()
