import datetime
import os

from dataflows import Flow, validate, update_resource
from dataflows import add_metadata, dump_to_path, load, set_type


def readme(fpath='README.md'):
    if os.path.exists(fpath):
        return open(fpath).read()


def format_date(row):
    if row.get('Date'):
        # Float returned by XLS file is exactly 693594 less then ordinal number in python
        pre_date = datetime.date(1997, 1, 7).fromordinal(int(row.get('Date') + 693594))
        formated_date = datetime.datetime.strptime((str(pre_date)), "%Y-%m-%d").strftime('%Y-%m-%d')
        row['Date'] = formated_date


oil_prices = Flow(
    add_metadata(
        name="oil-prices",
        title= "Brent and WTI Spot Prices",
        descriptor="A variety of temporal granularities for Europe Brent and WTI (West Texas Intermediate) Spot Prices.",
        sources=[
            {
                "name": "Daily Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls",
                "title": "Daily Europe Brent Spot Price"
            },
            {
                "name": "Weekly Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEw.xls",
                "title": "Weekly Europe Brent Spot Price"
            },
            {
                "name": "Monthly Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEm.xls",
                "title": "Monthly Europe Brent Spot Price"
            },
            {
                "name": "Annual Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEa.xls",
                "title": "Annual Europe Brent Spot Price"
            },
            {
                "name": "Daily Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls",
                "title": "Daily Cushing, OK WTI Spot Price"
            },
            {
                "name": "Weekly Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCw.xls",
                "title": "Weekly Cushing, OK WTI Spot Price"
            },
            {
                "name": "Monthly Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCm.xls",
                "title": "Monthly Cushing, OK WTI Spot Price"
            },
            {
                "name": "Annual Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCa.xls",
                "title": "Annual Cushing, OK WTI Spot Price"
            }
        ],
        licenses=[
            {
                "name": "ODC-PDDL-1.0",
                "path": "http://opendatacommons.org/licenses/pddl/",
                "title": "Open Data Commons Public Domain Dedication and License v1.0"
            }
        ],
        keywords=["Oil","Brent","WTI","Oil Prices","eia","oil eia"],
        views=[
            {
                "name": "graph",
                "title": "Europe Brent Spot Price FOB (Dollars per Barrel)",
                "resourceName": "brent-day",
                "specType": "simple",
                "spec": {
                "type": "line",
                    "group": "Date",
                    "series": ["Brent Spot Price"]
                }
            }
        ],
        readme=readme()
    ),
    load(
        load_source='https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='brent-daily'
    ),
    load(
        load_source='https://www.eia.gov/dnav/pet/hist_xls/RBRTEw.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='brent-weekly'
    ),
    load(
        load_source='https://www.eia.gov/dnav/pet/hist_xls/RBRTEm.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='brent-monthly'
    ),
    load(
        load_source='https://www.eia.gov/dnav/pet/hist_xls/RBRTEa.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='brent-annual'
    ),
    load(
        load_source='http://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='wti-daily'
    ),
    load(
        load_source='http://www.eia.gov/dnav/pet/hist_xls/RWTCw.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='wti-weekly'
    ),
    load(
        load_source='http://www.eia.gov/dnav/pet/hist_xls/RWTCm.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='wti-monthly'
    ),
    load(
        load_source='http://www.eia.gov/dnav/pet/hist_xls/RWTCa.xls',
        format='xls',
        sheet=2,
        skip_rows=[1,2,3],
        headers=['Date', 'Price'],
        name='wti-annual'
    ),
    update_resource('brent-daily', **{'path':'data/brent-daily.csv', 'dpp:streaming': True}),
    update_resource('brent-weekly', **{'path':'data/brent-weekly.csv', 'dpp:streaming': True}),
    update_resource('brent-monthly', **{'path':'data/brent-monthly.csv', 'dpp:streaming': True}),
    update_resource('brent-annual', **{'path':'data/brent-annual.csv', 'dpp:streaming': True}),
    update_resource('wti-daily', **{'path':'data/wti-daily.csv', 'dpp:streaming': True}),
    update_resource('wti-weekly', **{'path':'data/wti-weekly.csv', 'dpp:streaming': True}),
    update_resource('wti-monthly', **{'path':'data/wti-monthly.csv', 'dpp:streaming': True}),
    update_resource('wti-annual', **{'path':'data/wti-annual.csv', 'dpp:streaming': True}),
    format_date,
    set_type('Date', resources=None, type='date', format='any'),
    validate(),
    dump_to_path(),
)


def flow(parameters, datapackage, resources, stats):
    return oil_prices


if __name__ == '__main__':
    oil_prices.process()
