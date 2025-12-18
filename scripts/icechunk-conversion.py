import fsspec
from virtualizarr import open_virtual_mfdataset
from virtualizarr.parsers import NetCDF3Parser
from virtualizarr.registry import ObjectStoreRegistry

import obstore
import icechunk
import xarray as xr
import pandas as pd
import json
import shutil

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
prefix = "jbusecke/RASI/delivery"
data_dir_root = "s3://nasa-waterinsight/RASI/"
# experiment_ids = ["HISTORICAL", "SSP245", "SSP585"]
experiment_ids = ["SSP585"]
# variables = ['RZSM_percentiles', 'SWE_percentiles', 'SnowDepth_percentiles', 'TotalPrecip_percentiles', 'Snowf_percentiles', 'Qs_percentiles', 'Streamflow_percentiles', 'AvgSurfT_percentiles', 'Qsb_percentiles']
# Excluding streamflow for now
variables = [
    "RZSM_percentiles",
    "SWE_percentiles",
    "SnowDepth_percentiles",
    "TotalPrecip_percentiles",
    "Snowf_percentiles",
    "Qs_percentiles",
    "AvgSurfT_percentiles",
    "Qsb_percentiles",
]

## Produce a virtual dataset from the list of files
bucket = "s3://nasa-waterinsight"
store = obstore.store.from_url(bucket, region="us-west-2", skip_signature=True)

registry = ObjectStoreRegistry({bucket: store})

parser = NetCDF3Parser()

files_dict = {}
# Contstruct files and check numbering (I suspect that some are actually missing!)
for experiment_id in experiment_ids:
    files_dict[experiment_id] = {}
    ## Find Files of interest
    data_dir = f"{data_dir_root}**/{experiment_id}/"
    print(f"Processing {data_dir}")
    # Use fsspec to list files in the S3 bucket
    fs = fsspec.filesystem("s3", anon=True)

    files = fs.glob(data_dir + "**/*.nc")

    for var in variables:
        var_files = [f for f in files if var in f]

        # not necessary if we do not virtualize 'Streamflow_percentiles'
        # # TEMPORARY FIX (filter out years < and >2082 to avoid the time mismatch with Streamflow_percentiles (https://github.com/virtual-zarr/rasi-icechunk/issues/5)
        # print('filtering files to align time as temporary fix')
        # var_files_filtered = []
        # for f in var_files:
        #     year = int(f.replace('.nc','').split('_')[-1][0:4])
        #     if year >= 1951 and year <= 2082:
        #         var_files_filtered.append(f)
        # var_files = var_files_filtered
        files_dict[experiment_id][var] = var_files

for experiment_id in experiment_ids:
    print(f"Creating Store for {experiment_id=}")
    store_prefix = f"{prefix}/{experiment_id}/"

    var_dict = {}
    for var in files_dict[experiment_id].keys():
        files = files_dict[experiment_id][var]
        print(f"{len(files)} files found")
        urls = ["s3://" + file for file in files]
        print(f"Virtualizing {var=}")
        var_vds = open_virtual_mfdataset(
            urls,
            parser=parser,
            registry=registry,
            # parallel="lithops",
            parallel="dask",
            preprocess=preprocess,
            combine_attrs=combine_attrs,
            loadable_variables=["date", "lon", "lat", "percentile"],
        )

        print(f"Single variable virtual Dataset: {var_vds}")

        print("Checking variables and parsing to datarray")
        # the filenames and variable names use inconsistent upper/lower case for percentile
        # use whatever is the actual variable name is
        assert len(var_vds.data_vars) == 1
        var_fixed = list(var_vds.data_vars)[0]

        print(f"Variable from file {var} vs variable from data {var_fixed}")
        if var_fixed.lower() != var.lower():
            raise ValueError("Variables do not match")

        var_vda = var_vds[var_fixed]
        var_dict[var_fixed] = var_vda
        print(f"Single variable dataset {var_vda}")

        # this errored out before! Quick fix for now:
        # Does open_virtual_mdfdataset need a cleanup call?
        import os

        if os.path.exists("/tmp/lithops-root/lithops"):
            shutil.rmtree("/tmp/lithops-root/lithops")

    # vds = xr.Dataset(var_dict)
    vds = xr.merge(var_dict.values())
    print(f"Virtual Merged Dataset {vds}")

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
