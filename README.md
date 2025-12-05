# RASI-icechunk

Creating virtual icechunk stores for [NASA RASI](https://www.nasa.gov/rasi/) dataset.

The virtual icechunk repository is hosted publicly at `s3://nasa-waterinsight/virtual-zarr-store/icechunk/RASI/`.

## Usage Example
```python
import icechunk
import xarray as xr

storage = icechunk.s3_storage(
    bucket='nasa-waterinsight',
    prefix=f"virtual-zarr-store/icechunk/RASI/HISTORICAL", #replace HISTORICAL with SSP245/SSP585 for future scenarios
    anonymous=True,
    region="us-west-2",
)

chunk_url = "s3://nasa-waterinsight/RASI/"
virtual_credentials = icechunk.containers_credentials({
    chunk_url: icechunk.s3_anonymous_credentials()
})

repo = icechunk.Repository.open(
    storage=storage,
    authorize_virtual_chunk_access=virtual_credentials,
)

session = repo.readonly_session('main')
ds = xr.open_zarr(session.store, consolidated=False, zarr_version=3, chunks={})
ds
```


## Dependency management

This repo uses [uv](https://docs.astral.sh/uv/) as package/project manager.


### Running Jupyter Notebooks on the NASA VEDA hub

To reproduce results in the notebooks, you need to build a custom kernel with

```
uv sync
uv run bash
python -m ipykernel install --user --name=rasienv --display-name="RASI-VENV"
```

Then select the "LNDAS-VENV" kernel on the upper right corner drop-down in your notebook (you might have to refresh the browser to see it).

### Running scripts

You can run the scripts with
```
uv run <scriptname>
```
