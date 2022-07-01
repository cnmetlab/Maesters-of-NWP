# Maesters - Nnumercial Weather Prediction
![](https://badgen.net/pypi/v/maesters-nwp) ![](https://badgen.net/badge/license/MIT/pink) ![](https://badgen.net/badge/github/blizhan/purple?icon=github) ![](https://badgen.net/https/cal-badge-icd0onfvrxx6.runkit.sh/Asia/Shanghai)

![](https://raw.githubusercontent.com/blizhan/Maesters-of-NWP/main/pics/usage.png)

A package focus on fecth open-source global numerical weather prediction product in a elegant way. 

The following data sources are supported.
  
  ✔︎ Deutscher Wetterdientst - ICON
  ✔︎ European Centre for Medium-Range Weather Forecasts - OPER / ENFO
  ✔︎ Canadian Meteorological Center - GEM / GEPS

The following data sources support are undergoing!
  ❏ National Oceanic and Atmospheric Adminstration - GFS
  ❏ Met Office - MOGREPS



## How to install

### Dependence
1. cdo,curl (install[cdo](https://anaconda.org/conda-forge/cdo), [curl](https://anaconda.org/conda-forge/curl))
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

ec = Maester('ecmwf','oper','2022-06-29 12:00',hour=[6,30],varname='TP_L0')

# get xarray object
ec.xarray()

# or only download (if lcoal_dir is not given, default download to $HOME/data/{source}/{product}/{date:%Y%m%d%H0000}/)
ec.download(local_dir='./') 
```
### Problem List
problem1: pyporj instal fail on M1 chip

![](https://raw.githubusercontent.com/blizhan/Maesters-of-NWP/main/pics/p1_desc.png)
solve1: 
```shell
brew install proj
pip install pyproj
```

