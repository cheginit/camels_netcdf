#!/usr/bin/env python
"""Convert CAMELS data to Zarr format."""
import pandas as pd
from pathlib import Path
import xarray as xr
import geopandas as gpd
import warnings

warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')

ATTR_DIR = Path("data", "camels_attributes_v2.0")
BASIN_DIR = Path("data", "basin_set_full_res")
QOBS_DIR = Path("data", "basin_timeseries_v1p2_metForcing_obsFlow", "basin_dataset_public_v1p2", "usgs_streamflow")

def read_basin() -> gpd.GeoDataFrame:
    """Read the basin shapefile."""
    print("Reading basin geometries ...")
    basin = gpd.read_file(Path(BASIN_DIR, "HCDN_nhru_final_671.shp"))
    basin = basin.to_crs("epsg:4326")
    basin["hru_id"] = basin.hru_id.astype(str).str.zfill(8)
    return basin.set_index("hru_id").geometry

def read_attributes() -> pd.DataFrame:
    """Convert all the attributes to a single dataframe."""
    print("Reading basin attributes ...")
    attr_files = Path(ATTR_DIR).glob("camels_*.txt")
    attrs = {f.stem.split("_")[1]: pd.read_csv(f, sep=";", index_col=0, dtype={"huc_02": str, "gauge_id": str}) for f in attr_files}

    attrs_df = pd.concat(attrs.values(), axis=1)
    def fix_station_nm(station_nm):
        name = station_nm.title().rsplit(" ", 1)
        name[0] = name[0] if name[0][-1] == "," else f"{name[0]},"
        name[1] = name[1].replace(".", "")
        return " ".join((name[0], name[1].upper() if len(name[1]) == 2 else name[1].title()))

    attrs_df["gauge_name"] = [fix_station_nm(n) for n in attrs_df["gauge_name"]]
    for c in attrs_df.columns[attrs_df.dtypes == "object"]:
        attrs_df[c] = attrs_df[c].str.strip()
    
    basin = read_basin()
    return gpd.GeoDataFrame(attrs_df, geometry=basin, crs="epsg:4326")

def read_qobs(qobs_txt: Path) -> pd.DataFrame:
    """Read the streamflow data."""
    qobs = pd.read_csv(qobs_txt, delim_whitespace=True, header=None, index_col=0, dtype={0: str})
    qobs = qobs.rename(columns={1: "Year", 2: "Month", 3: "Day", 4: qobs.index[0], 5: "Quality"})
    qobs["time"] = pd.to_datetime(qobs[['Year', 'Month', 'Day']])
    qobs = qobs.drop(columns=["Year", "Month", "Day", "Quality"])
    qobs = qobs.set_index("time")
    return qobs

attrs = read_attributes()
attrs.to_feather("camels_attributes_v2.0.feather")

print("Reading basin streamflow data ...")
qobs = pd.concat((read_qobs(Path(QOBS_DIR, huc, f"{sid}_streamflow_qc.txt")) for sid, huc in attrs["huc_02"].items()), axis=1)
ds = xr.Dataset(
    data_vars={
        "discharge": (["time", "station_id"], qobs),
        **{
            attr: (["station_id"], v)
            for attr, v in attrs.drop(columns="geometry").items()
        },
    },
    coords={
        "time": qobs.index.to_numpy(),
        "station_id": qobs.columns,
    },
)
ds["discharge"].attrs["units"] = "cfs"

ds.to_zarr(Path("camels_attrs_v2_streamflow_v1p2.zarr"), mode="w", consolidated=True)