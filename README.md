# CAMELS dataset in NetCDF/Feather formats

## Motivation

The CAMELS datasets are now provided in an ideal format and takes
a bit of data processing to convert them to useful and convenient
dataframes. So, I decided to use the beloved `netcdf` and `feather`
formats to make the dataset more accessible!

## Usage

First make sure that `h5netcdf`, `geopandas`, `xarray`, and `requests`
Python packages are installed, then load the files directly like so:

```python
import io

import geopandas as gpd
import requests
import xarray as xr

r = requests.get("https://media.githubusercontent.com/media/cheginit/camels_netcdf/main/camels_attributes_v2.0.feather")
attrs = gpd.read_feather(io.BytesIO(r.content))

r = requests.get("https://media.githubusercontent.com/media/cheginit/camels_netcdf/main/camels_attrs_v2_streamflow_v1p2.nc")
qobs = xr.open_dataset(io.BytesIO(r.content), engine="h5netcdf")
```

## Methodology

This repo contains the code that I used to generate the datasets.
Two data sources are available from the CAMELS dataset:

- Streamflow observations for all 671 stations in the
  [CAMELS](https://ral.ucar.edu/solutions/products/camels) dataset.
- All the watershed attributes (`camels_attributes_v2.0`).

The `camel_netcdf.py` generate two files:

- `camels_attributes_v2.0.feather`: Includes basin geometries and 60
  basin-level attributes that are available in CAMELS.
- `camels_attrs_v2_streamflow_v1p2.nc`: A `xarray.Dataset`
  that includes streamflow observations for all 671 stations, as well
  as the 60 basin-level attributes.

Additionally, the script takes care of some small annoyances in the dataset:

- Station names didn't have a consistent format and there were some missing
  commas and extra periods! Now, the names have a consistent format (`title`)
  and there is comma before the states.
- Station IDs and HUC 02 are strings with leading zero if needed.

Although, the generated `netcdf` and `feather` files are available in this repo,
you can recreate them locally using
[`mambaforge`](https://github.com/conda-forge/miniforge/) (or `conda`) like so:

```bash
mamba env create -f environment.yml
conda activate camels
chmod +x ./camels_netcdf.py
./camels_netcdf.py
```

## Example Plots

Snow fraction using `camels_attributes_v2.0.feather`:
![camels_snow_fraction](plots/camels_snow_fraction.png)

The dataset `camels_attrs_v2_streamflow_v1p2.nc`:
![dataset](plots/dataset.png)

Streamflow observations for USGS-01013500:
![qobs_01013500](plots/qobs_01013500.png)

## Contributions

Contributions are welcome! Please feel free to open an issue/PR if you
have any suggestions that can improve the database.
