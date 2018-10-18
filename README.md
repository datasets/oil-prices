Europe Brent and WTI (Western Texas Intermediate) Spot Prices (Annual/ Monthly/ Weekly/ Daily) from EIA U.S. (Energy Information Administration).

## Data

This series is available through the EIA open data API

- [Europe Brent Spot Price FOB (Dollars per Barrel)](https://www.eia.gov/dnav/pet/hist/RBRTEd.htm) - From 20 May 1987 till today
- [Cushing, OK WTI Spot Price FOB (Dollars per Barrel)](https://www.eia.gov/dnav/pet/hist/RWTCD.htm) - From 01 February 1986 till today

### Definitions

#### Brent
> A blended crude stream produced in the North Sea region which serves as a reference or "marker" for pricing a number of other crude streams.
[source](https://www.eia.gov/dnav/pet/TblDefs/pet_pri_spt_tbldef2.asp)

#### West Texas Intermediate (WTI - Cushing)
> A crude stream produced in Texas and southern Oklahoma which serves as a reference or "marker" for pricing a number of other crude streams and which is traded in the domestic spot market at Cushing, Oklahoma.
[source](https://www.eia.gov/dnav/pet/TblDefs/pet_pri_spt_tbldef2.asp)

## Preparation

You will need Python 3.6 or greater and dataflows library to run the script

To update the data run the process script locally:

```
# Install dataflows
pip install dataflows

# Run the script
python oil_prices_flow.py
```

## License

> U.S. government publications are in the public domain and are not subject to copyright protection. You may use and/or distribute any of our data, files, databases, reports, graphs, charts, and other information products that are on our website or that you receive through our email distribution service. However, if you use or reproduce any of our information products, you should use an acknowledgment, which includes the publication date, such as: "Source: U.S. Energy Information Administration (Oct 2008)."

You may find further information [here](https://www.eia.gov/about/copyrights_reuse.cfm)

### Additional work

> All the additional work made to build this Data Package is made available under the Public Domain Dedication and License v1.0 whose full text can be found at: http://www.opendatacommons.org/licenses/pddl/1.0/
