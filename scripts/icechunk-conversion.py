import fsspec
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import NetCDF3Parser
from virtualizarr.registry import ObjectStoreRegistry

import obstore
import icechunk
import xarray as xr
import pandas as pd

## Find Files of interest

data_dir = "s3://nasa-waterinsight/test/CASI/"
# change these as needed.
store_bucket = "nasa-veda-scratch"
store_prefix = "jbusecke/RASI-test/"


def preprocess(ds: xr.Dataset) -> xr.Dataset:
    # parse the int date into datetime and add as coordinate
    time = xr.DataArray(
        pd.to_datetime(ds.date.values.astype(str), format="%Y%m"), dims=["time"]
    )
    # drop the old coord
    ds = ds.drop("date")
    # expand the data vars and then assign the time coordinate
    ds = ds.expand_dims("time")
    ds = ds.assign_coords({"time": time})
    return ds


# Use fsspec to list files in the S3 bucket
fs = fsspec.filesystem("s3", anon=True)
files = fs.glob(data_dir + "**/*.nc")
print(f"{len(files)} found")

## Produce a virtual dataset from the list of files
bucket = "s3://nasa-waterinsight"
store = obstore.store.from_url(bucket, region="us-west-2", skip_signature=True)

registry = ObjectStoreRegistry({bucket: store})

parser = NetCDF3Parser()

urls = ["s3://" + file for file in files]
vds = open_virtual_mfdataset(
    urls,
    parser=parser,
    registry=registry,
    parallel="lithops",
    preprocess=preprocess,
    loadable_variables=["date", "lon", "lat", "percentile"],
)

print(f"Virtual Dataset: {vds}")

## Write (commit) the virtual dataset into icechunk
storage = icechunk.s3_storage(
    bucket=store_bucket,
    prefix=store_prefix,
    anonymous=False,
    from_env=True,
)

config = icechunk.RepositoryConfig.default()
config.set_virtual_chunk_container(
    icechunk.VirtualChunkContainer(
        data_dir,
        icechunk.s3_store(region="us-west-2"),
    )
)

virtual_credentials = icechunk.containers_credentials(
    {data_dir: icechunk.s3_anonymous_credentials()}
)

repo = icechunk.Repository.open_or_create(
    storage=storage,
    config=config,
    authorize_virtual_chunk_access=virtual_credentials,
)

session = repo.writable_session("main")
vds.vz.to_icechunk(session.store)
session.commit("First Commit")
print("DONE")
