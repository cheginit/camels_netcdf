#!/usr/bin/env python
"""Convert CAMELS data to netcdf/feather format."""
import warnings
import zipfile
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import pandas as pd
import xarray as xr
from urllib.request import urlopen

from downloader import download

warnings.filterwarnings("ignore", message=".*initial implementation of Parquet.*")

ROOT = Path("data")
ATTR_DIR = Path(ROOT, "camels_attributes_v2.0")
BASIN_DIR = Path(ROOT)
QOBS_DIR = Path(
    ROOT,
    "basin_dataset_public_v1p2",
    "usgs_streamflow",
)


def download_files() -> None:
    """Download the required zip files."""
    print("Downloading raw CAMELS files ...")
    ROOT.mkdir(exist_ok=True)
    base_url = "/".join(
        [
            "https://ral.ucar.edu/sites/default/files/public/product-tool",
            "camels-catchment-attributes-and-meteorology-for-large-sample-studies-dataset-downloads",
        ]
    )
    links = [
        f"{base_url}/camels_attributes_v2.0.zip",
        f"{base_url}/basin_set_full_res.zip",
        f"{base_url}/basin_timeseries_v1p2_metForcing_obsFlow.zip",
    ]
    to_dl = [url for url in links if not Path(ROOT, url.rsplit("/", 1)[1]).exists()]
    for url in to_dl:
        fzip = Path(ROOT, url.rsplit("/", 1)[1])
        if fzip.exists():
            with urlopen(url) as response:
                if int(response.info()["Content-length"]) != fzip.stat().st_size:
                    fzip.unlink()
    download(to_dl, ROOT)
    print("Extracting the downloaded files ...")
    for f in ROOT.glob("*.zip"):
        with zipfile.ZipFile(f) as zf:
            zf.extractall(ROOT)


def read_basin() -> gpd.GeoDataFrame:
    """Read the basin shapefile."""
    print("Reading basin geometries ...")
    basin = gpd.read_file(Path(ROOT, "HCDN_nhru_final_671.shp"))
    basin = basin.to_crs("epsg:4326")
    basin["hru_id"] = basin.hru_id.astype(str).str.zfill(8)
    return basin.set_index("hru_id").geometry


def read_attributes() -> Tuple[pd.DataFrame, pd.Index]:
    """Convert all the attributes to a single dataframe."""
    print("Reading basin attributes ...")
    attr_files = Path(ATTR_DIR).glob("camels_*.txt")
    attrs = {
        f.stem.split("_")[1]: pd.read_csv(
            f, sep=";", index_col=0, dtype={"huc_02": str, "gauge_id": str}
        )
        for f in attr_files
    }

    attrs_df = pd.concat(attrs.values(), axis=1)

    def fix_station_nm(station_nm):
        name = station_nm.title().rsplit(" ", 1)
        name[0] = name[0] if name[0][-1] == "," else f"{name[0]},"
        name[1] = name[1].replace(".", "")
        return " ".join((name[0], name[1].upper() if len(name[1]) == 2 else name[1].title()))

    attrs_df["gauge_name"] = [fix_station_nm(n) for n in attrs_df["gauge_name"]]
    obj_cols = attrs_df.columns[attrs_df.dtypes == "object"]
    for c in obj_cols:
        attrs_df[c] = attrs_df[c].str.strip().astype(str)

    basin = read_basin()
    return gpd.GeoDataFrame(attrs_df, geometry=basin, crs="epsg:4326"), obj_cols


def _read_qobs(qobs_txt: Path) -> pd.DataFrame:
    """Read the streamflow data."""
    qobs = pd.read_csv(qobs_txt, delim_whitespace=True, header=None, index_col=0, dtype={0: str})
    qobs = qobs.rename(columns={1: "Year", 2: "Month", 3: "Day", 4: qobs.index[0], 5: "Quality"})
    qobs["time"] = pd.to_datetime(qobs[["Year", "Month", "Day"]])
    qobs = qobs.drop(columns=["Year", "Month", "Day", "Quality"])
    qobs = qobs.set_index("time")
    return qobs


def read_qobs(attrs: pd.DataFrame, obj_cols: pd.Index) -> xr.Dataset:
    """Read the streamflow data."""
    print("Reading basin streamflow data ...")
    qobs = pd.concat(
        (
            _read_qobs(Path(QOBS_DIR, huc, f"{sid}_streamflow_qc.txt"))
            for sid, huc in attrs["huc_02"].items()
        ),
        axis=1,
    )
    ds = xr.Dataset(
        data_vars={
            "discharge": (["time", "station_id"], qobs),
            **{attr: (["station_id"], v) for attr, v in attrs.drop(columns="geometry").items()},
        },
        coords={
            "time": qobs.index.to_numpy(),
            "station_id": qobs.columns,
        },
    )
    ds["discharge"].attrs["units"] = "cfs"
    for v in obj_cols:
        ds[v] = ds[v].astype(str)
    return ds


if __name__ == "__main__":
    download_files()
    attrs, obj_cols = read_attributes()
    attrs.to_feather("camels_attributes_v2.0.feather")
    ds = read_qobs(attrs, obj_cols)
    ds.to_netcdf("camels_attrs_v2_streamflow_v1p2.nc", engine="h5netcdf")
