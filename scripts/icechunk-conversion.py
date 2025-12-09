import fsspec
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import NetCDF3Parser
from virtualizarr.registry import ObjectStoreRegistry

import obstore
import icechunk
import xarray as xr
import pandas as pd
import json

from lithops.config import load_config

# Load the full configuration
config = load_config()
print(config)

# Print it nicely formatted


print(json.dumps(config, indent=2, default=str))


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


# change these as needed.
store_bucket = "nasa-veda-scratch"
data_dir_root = "s3://nasa-waterinsight/RASI/"

experiment_ids = ["HISTORICAL", "SSP245", "SSP585"]

for experiment_id in experiment_ids[0:1]:
    ## Find Files of interest
    data_dir = f"{data_dir_root}**/{experiment_id}/"
    print(f"Processing {data_dir}")
    store_prefix = f"jbusecke/RASI/test/{experiment_id}/"

    # Use fsspec to list files in the S3 bucket
    fs = fsspec.filesystem("s3", anon=True)
    # files = fs.glob(data_dir + "**/*.nc")
    if experiment_id == "HISTORICAL":
        files = fs.glob(data_dir + "**/*1951*.nc")
    else:
        files = fs.glob(data_dir + "**/*2015*.nc")

    print(f"{len(files)} found")

    # Detect variables
    unique_variables = list(
        set(["_".join(f.split("/")[-1].split("_")[0:-1]) for f in files])
    )
    print(f"{unique_variables=}")

    ## Produce a virtual dataset from the list of files
    bucket = "s3://nasa-waterinsight"
    store = obstore.store.from_url(bucket, region="us-west-2", skip_signature=True)

    registry = ObjectStoreRegistry({bucket: store})

    parser = NetCDF3Parser()

    urls = ["s3://" + file for file in files]
    var_dict = {}
    for var in unique_variables:
        print(var)
        # Test that one variable only works
        var_urls = [u for u in urls if var in u]
        var_vds = open_virtual_mfdataset(
            var_urls,
            parser=parser,
            registry=registry,
            parallel="lithops",
            preprocess=preprocess,
            combine_attrs=combine_attrs,
            loadable_variables=["date", "lon", "lat", "percentile"],
        )
        # the filenames and variable names use inconsistent upper/lower case for percentile
        # use whatever is the actual variable name is
        assert len(var_vds.data_vars) == 1
        var_fixed = list(var_vds.data_vars)[0]

        print(f"Virtual Dataset: {var_vds}")
        var_vda = var_vds[var_fixed]
        var_dict[var_fixed] = var_vda

    vds = xr.Dataset(var_dict)

    ## Open or create icechunk store
    storage = icechunk.s3_storage(
        bucket=store_bucket,
        prefix=store_prefix,
    )

    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            data_dir_root,
            icechunk.s3_store(region="us-west-2"),
        )
    )

    virtual_credentials = icechunk.containers_credentials(
        {data_dir_root: icechunk.s3_anonymous_credentials()}
    )
    print(f"Open Repo at {store_prefix}")
    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=virtual_credentials,
    )

    session = repo.writable_session("main")
    vds.vz.to_icechunk(session.store)
    session.commit(f"Add {var}")
    print(f"{store_prefix}: DONE")
