# RASI-icechunk

Creating virtual icechunk stores for [NASA RASI](https://www.nasa.gov/rasi/) dataset.

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
