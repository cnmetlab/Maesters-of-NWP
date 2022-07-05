# Maesters - Numercial Weather Prediction
![](https://badgen.net/pypi/v/maesters-nwp) ![](https://badgen.net/badge/license/MIT/pink) ![](https://badgen.net/badge/github/blizhan/purple?icon=github) ![](https://badgen.net/https/cal-badge-icd0onfvrxx6.runkit.sh/Asia/Shanghai)
![](https://zenodo.org/badge/doi/10.5281/zenodo.6796046.svg)
![](https://raw.githubusercontent.com/blizhan/Maesters-of-NWP/main/pics/usage_20220704.png)

A package focus on fecth open-source global numerical weather prediction product in a elegant way. 


The following data sources are supported.
  
  ✔︎ _Deutscher Wetterdientst_ - **ICON**

  ✔︎ _European Centre for Medium-Range Weather Forecasts_ - **OPER** / **ENFO**

  ✔︎ _Canadian Meteorological Center_ - **GEM** / **GEPS**

The following data sources support is coming. 🚀🚀🚀

  ❏ _National Oceanic and Atmospheric Adminstration_ - GFS

  ❏ _Met Office_ - MOGREPS



## How to install

### Dependence
1. cdo,curl (install [cdo](https://anaconda.org/conda-forge/cdo), [curl](https://anaconda.org/conda-forge/curl))
```shell
conda install -c conda-forge cdo curl
```


### Install
```shell
pip install maesters-nwp
```
### Usage
``` python
from maester import Maesters

ec = Maester(source='ecmwf', product='oper', date='2022-06-29 12:00',hour=[6,30],varname='TP_SFC')

# get xarray object
ec.xarray()

# or only download (if lcoal_dir is not given, default download to current dir)
ec.download(local_dir='./') 

# or operation download all data of the newest batch, default download to $HOME/data/{source}/{product}/{batch:%Y%m%d%H0000}
ec.operation(local_dir='./')

```
### Problem List
_<font color=#008000 >P1: </font>_ _pyporj instal fail on M1 chip_

![](https://raw.githubusercontent.com/blizhan/Maesters-of-NWP/main/pics/p1_desc.png)
_<font color=#008000 >S1: </font>_ 
```shell
brew install proj
pip install pyproj
```

### Citation
If this package give helps to your research or work, it will be a enjoyable thing to the contributors of this package. And if you are willing to cite the contribution of this package in your publication, you can find the DOI information at https://doi.org/10.5281/zenodo.6796046.