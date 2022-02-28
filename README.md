# CAMELS dataset in Zarr/Feather formats

## Motivation

The CAMELS datasets are now provided in an ideal format and takes
a bit of data processing to convert them to useful and convenient
dataframes. So, I decided to use the beloved `zarr` and `feather`
formats to make the dataset more accessible!

## Methodology

This repo contains the code that I used to generate the datasets.
Two data sources are available from the CAMELS dataset:

- Streamflow observations for all 671 stations in the
  [CAMELS](https://ral.ucar.edu/solutions/products/camels) dataset.
- All the watershed attributes (`camels_attributes_v2.0`).

The `camel_zarr.py` generate two files:

- `camels_attributes_v2.0.feather`: Includes basin geometries and 60
  basin-level attributes that are available in CAMELS.
- `camels_attrs_v2_streamflow_v1p2.zarr`: A `xarray.Dataset`
  that includes streamflow observations for all 671 stations, as well
  as the 60 basin-level attributes.

Additionally, the script takes care of some small annoyances in the dataset:

- Station names didn't have a consistent format and there were some missing
  commas and extra periods! Now, the names have a consistent format (`title`)
  and there is comma before the states.
- Station IDs and HUC 02 are strings with leading zero if needed.

Although, the generated `zarr` and `feather` files are available in this repo,
you can recreate them locally using
[`mambaforge`](https://github.com/conda-forge/miniforge/) (or `conda`) like so:

```bash
mamba env create -f environment.yml
conda activate camels
chmod +x ./camels_zarr.py
./camels_zarr.py
```

## Usage

You can load the files directly like so:

```python
import xarray as xr
import geopandas as gpd

attrs = gpd.read_feather("https://raw.githubusercontent.com/cheginit/camels_zarr/main/camels_attributes_v2.0.feather")
qobs = xr.open_zarr("https://raw.githubusercontent.com/cheginit/camels_zarr/main/camels_attrs_v2_streamflow_v1p2.zarr")
```

## Example Plots

Snow fraction using `camels_attributes_v2.0.feather`:
![camels_snow_fraction](plots/camels_snow_fraction.png)

The dataset `camels_attrs_v2_streamflow_v1p2.zarr`:
![dataset](plots/dataset.png)

Streamflow observations for USGS-01013500:
![qobs_01013500](plots/qobs_01013500.png)

## Contributions

Contributions are welcome! Please feel free to open an issue/PR if you
have any suggestions that can improve the database.