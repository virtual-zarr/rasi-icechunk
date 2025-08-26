import fsspec
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import NetCDF3Parser
from virtualizarr.registry import ObjectStoreRegistry

import obstore
import icechunk
import xarray as xr
import pandas as pd


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


def combine_attrs(dicts, context):
    combined_attrs = {}

    # Get keys from first dict as reference
    all_keys = set(dicts[0].keys())

    # Check that every key exists in all dicts
    for i, d in enumerate(dicts[1:], 1):
        if set(d.keys()) != all_keys:
            missing = all_keys - set(d.keys())
            extra = set(d.keys()) - all_keys
            raise KeyError(f"Dict {i} key mismatch. Missing: {missing}, Extra: {extra}")

    for key in all_keys:
        values = [d[key] for d in dicts]
        unique_values = set(values)

        if len(unique_values) == 1:
            # All values are the same
            combined_attrs[key] = values[0]
        else:
            raise ValueError(
                f"No instructions provided how to handle {key=} with {values=}"
            )

    return combined_attrs


experiment_ids = ["HISTORICAL", "SSP245", "SSP585"]

# change these as needed.
store_bucket = "nasa-veda-scratch"

for experiment_id in experiment_ids:
    ## Find Files of interest
    data_dir = f"s3://nasa-waterinsight/RASI/ROUTING/{experiment_id}/"
    print(f"Processing {data_dir}")
    store_prefix = f"jbusecke/RASI/test/{experiment_id}/"

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
        combine_attrs=combine_attrs,
        loadable_variables=["date", "lon", "lat", "percentile"],
    )

    print(f"Virtual Dataset: {vds}")
    ## Write (commit) the virtual dataset into icechunk
    storage = icechunk.s3_storage(
        bucket=store_bucket,
        prefix=store_prefix,
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
    print(f"Open Repo at {store_prefix}")
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=virtual_credentials,
    )

    session = repo.writable_session("main")
    vds.vz.to_icechunk(session.store)
    session.commit("First Commit")
    print(f"{store_prefix}: DONE")
