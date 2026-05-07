from dataflows import (
    Flow,
    PackageWrapper,
    validate,
)
from dataflows import add_metadata, dump_to_path, load, set_type, printer


_RESOURCE_META = [
    ("brent-daily",   "data/brent-daily.csv",   "Europe Brent crude oil spot price, daily observations, in USD per barrel (FOB). Data start: May 1987."),
    ("brent-week",    "data/brent-weekly.csv",   "Europe Brent crude oil spot price, weekly averages, in USD per barrel (FOB). The Date is the Friday ending each week. Data start: May 1987."),
    ("brent-month",   "data/brent-monthly.csv",  "Europe Brent crude oil spot price, monthly averages, in USD per barrel (FOB). The Date is reported as the 15th of each month. Data start: May 1987."),
    ("brent-year",    "data/brent-year.csv",     "Europe Brent crude oil spot price, annual averages, in USD per barrel (FOB). The Date is reported as June 30 of each year. Data start: 1987."),
    ("wti-daily",     "data/wti-daily.csv",      "WTI (West Texas Intermediate) crude oil spot price at Cushing, OK, daily observations, in USD per barrel (FOB). Data start: January 1986."),
    ("wti-week",      "data/wti-weekly.csv",     "WTI (West Texas Intermediate) crude oil spot price at Cushing, OK, weekly averages, in USD per barrel (FOB). The Date is the Friday ending each week. Data start: January 1986."),
    ("wti-month",     "data/wti-monthly.csv",    "WTI (West Texas Intermediate) crude oil spot price at Cushing, OK, monthly averages, in USD per barrel (FOB). The Date is reported as the 15th of each month. Data start: January 1986."),
    ("wti-year",      "data/wti-year.csv",       "WTI (West Texas Intermediate) crude oil spot price at Cushing, OK, annual averages, in USD per barrel (FOB). The Date is reported as June 30 of each year. Data start: 1986."),
]


def rename_resources(package: PackageWrapper):
    for i, (name, path, description) in enumerate(_RESOURCE_META):
        package.pkg.descriptor["resources"][i]["name"] = name
        package.pkg.descriptor["resources"][i]["path"] = path
        package.pkg.descriptor["resources"][i]["description"] = description

    yield package.pkg
    res_iter = iter(package)
    for res in res_iter:
        yield res.it
    yield from package


def filter_out_empty_rows(rows):
    for row in rows:
        if row["Date"]:
            yield row


OIL_PRICES = Flow(
    add_metadata(
        name="oil-prices",
        title="Brent and WTI Spot Prices",
        description=(
            "A variety of temporal granularities for Europe Brent and WTI "
            "(West Texas Intermediate) Spot Prices."
        ),
        sources=[
            {
                "name": "Daily Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls",
                "title": "Daily Europe Brent Spot Price",
            },
            {
                "name": "Weekly Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEw.xls",
                "title": "Weekly Europe Brent Spot Price",
            },
            {
                "name": "Monthly Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEm.xls",
                "title": "Monthly Europe Brent Spot Price",
            },
            {
                "name": "Annual Europe Brent Spot Price",
                "path": "https://www.eia.gov/dnav/pet/hist_xls/RBRTEa.xls",
                "title": "Annual Europe Brent Spot Price",
            },
            {
                "name": "Daily Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls",
                "title": "Daily Cushing, OK WTI Spot Price",
            },
            {
                "name": "Weekly Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCw.xls",
                "title": "Weekly Cushing, OK WTI Spot Price",
            },
            {
                "name": "Monthly Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCm.xls",
                "title": "Monthly Cushing, OK WTI Spot Price",
            },
            {
                "name": "Annual Cushing, OK WTI Spot Price",
                "path": "http://www.eia.gov/dnav/pet/hist_xls/RWTCa.xls",
                "title": "Annual Cushing, OK WTI Spot Price",
            },
        ],
        licenses=[
            {
                "name": "ODC-PDDL-1.0",
                "path": "http://opendatacommons.org/licenses/pddl/",
                "title": "Open Data Commons Public Domain Dedication and License v1.0",
            }
        ],
        keywords=["Oil", "Brent", "WTI", "Oil Prices", "eia", "oil eia"],
        views=[
            {
                "name": "brent-price-history",
                "title": "Brent Crude Oil Price (1987–present)",
                "description": "Weekly Brent crude spot price in USD per barrel. Major shocks stand out: the 1990 Gulf War spike, the 2008 financial crisis peak ($147), the 2014 OPEC supply glut collapse, the 2020 COVID demand crash, the 2022 Russia-Ukraine war surge, and the 2026 Iran war spike.",
                "resources": ["brent-week"],
                "specType": "plot",
                "spec": {
                    "dateFields": ["Date"],
                    "height": 400,
                    "marginLeft": 60,
                    "x": {"label": None},
                    "y": {"label": "Price (USD per barrel)", "tickFormat": "$,.0f", "grid": True},
                    "marks": [
                        {
                            "type": "ruleX",
                            "staticData": [
                                {"x": "1990-08-01"}, {"x": "2008-07-01"},
                                {"x": "2014-11-01"}, {"x": "2020-04-01"}, {"x": "2022-03-01"},
                                {"x": "2026-02-28"},
                            ],
                            "x": "x", "stroke": "#e5e7eb", "tip": False,
                        },
                        {
                            "type": "line",
                            "x": "Date", "y": "Price",
                            "stroke": "#be123c", "strokeWidth": 1.5, "tip": True,
                        },
                        {
                            "type": "text",
                            "staticData": [
                                {"x": "1990-08-01", "y": 130, "label": "Gulf War"},
                                {"x": "2008-07-01", "y": 130, "label": "$147 peak"},
                                {"x": "2014-11-01", "y": 130, "label": "OPEC glut"},
                                {"x": "2020-04-01", "y": 130, "label": "COVID"},
                                {"x": "2022-03-01", "y": 130, "label": "Russia-Ukraine"},
                                {"x": "2026-02-28", "y": 130, "label": "Iran war"},
                            ],
                            "x": "x", "y": "y", "text": "label",
                            "fill": "#6b7280", "fontSize": 10, "textAnchor": "start", "rotate": -45,
                        },
                    ],
                },
            },
        ],
    ),
    load(
        load_source="https://www.eia.gov/dnav/pet/hist_xls/RBRTEd.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    load(
        load_source="https://www.eia.gov/dnav/pet/hist_xls/RBRTEw.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    load(
        load_source="https://www.eia.gov/dnav/pet/hist_xls/RBRTEm.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    load(
        load_source="https://www.eia.gov/dnav/pet/hist_xls/RBRTEa.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    load(
        load_source="http://www.eia.gov/dnav/pet/hist_xls/RWTCd.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    load(
        load_source="http://www.eia.gov/dnav/pet/hist_xls/RWTCw.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    load(
        load_source="http://www.eia.gov/dnav/pet/hist_xls/RWTCm.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    load(
        load_source="http://www.eia.gov/dnav/pet/hist_xls/RWTCa.xls",
        format="xls",
        sheet=2,
        skip_rows=[1, 2, 3],
        headers=["Date", "Price"],
    ),
    rename_resources,
    set_type("Date", resources=["brent-daily", "wti-daily"], type="date", format="default",
             description="Observation date in YYYY-MM-DD format."),
    set_type("Date", resources=["brent-week", "wti-week"], type="date", format="default",
             description="End-of-week date (Friday) in YYYY-MM-DD format."),
    set_type("Date", resources=["brent-month", "wti-month"], type="date", format="default",
             description="Reported as the 15th of the month in YYYY-MM-DD format; represents the monthly average period."),
    set_type("Date", resources=["brent-year", "wti-year"], type="date", format="default",
             description="Reported as June 30 of the year in YYYY-MM-DD format; represents the annual average period."),
    set_type("Price", resources=None, type="number",
             description="Spot price in US dollars per barrel (FOB)."),
    validate(),
    printer(),
    filter_out_empty_rows,
    dump_to_path(),
)


if __name__ == "__main__":
    OIL_PRICES.process()
