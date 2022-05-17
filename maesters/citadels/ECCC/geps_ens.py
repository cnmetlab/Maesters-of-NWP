from retrying import retry
from bs4 import BeautifulSoup
import pygrib
from loguru import logger
from typing import Callable

from multiprocessing import Pool
from subprocess import call
from datetime import datetime
import requests
import re
import os

MAESTERS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
logger.add(os.path.join(os.path.dirname(MAESTERS),'log/GEPS_ENS_{time:%Y%m%d}'),rotation='00:00',retention='3 days')

PARALLEL_NUM = 8

PRODUCT = 'raw' # products
GEPS_ENS_URL = 'https://dd.weather.gc.ca/ensemble/geps/grib2/{PRODUCT}/{batch}/{hour}/'

# FN Example: 'CMC_geps-raw_AFRAIN_SFC_0_latlon0p5x0p5_2022051400_P192_allmbrs.grib2'

VARNAME = {
    'NTAT':{
        'DSWRF': 'TSR'
    },
    'SFC':{
        'ACPCP': 'TCP',
        'APCP': 'TP',
        'SKINT': 'ST',
        'DSWRF': 'SSRD',
        'NSWRS': 'SSR',
        'TCDC': 'TCC',
        'RH': 'RHU',
    },
    'TGL':{
        'RH': 'RHU',
        'UGRD': 'U',
        'VGRD': 'V',
    },
    'ISBL':{
        'RH': 'RHU',
        'UGRD': 'U',
        'VGRD': 'V',
    }
}

HOURS = {
    'subseason': list(range(3,192,3))+list(range(192,768+6,6)),
    'medium': list(range(3,192,3))+list(range(192,384+6,6))
}

def get_files_dict(date:datetime,batch:int,hour:int):
    """ get GEPS_ENS Files at date/batch/hour

    Parameters:
        date: datetime, initial datetime (UTC)
        batch: int, batch_hour 0/12
        hour: int, predict hour from initial datetime
    return:
        dict
            {
                "{VARNAME}_{LEVEL}-{HOUR}":"{URL}"
            }
    """
    date = date.replace(hour=batch,minute=0,second=0)
    batch = str(batch).zfill(2)
    hour = str(hour).zfill(3)
    url = GEPS_ENS_URL.format(PRODUCT=PRODUCT,batch=batch,hour=hour)
    resp = requests.get(url)
    res_dict = {}
    if resp.status_code == 200:
        bs_items= BeautifulSoup(resp.text, 'html.parser')
        prod = 'prob'if PRODUCT == 'products' else 'raw'
        parse_type = f'CMC_geps-{prod}_([A-Z]+)_([A-Z]+)_([0-9A-Za-z]+)_([0-9A-Za-z]+)_([0-9]+)_P([0-9]+)_*'
        files_list = [i['href'] for i in bs_items.find_all('a', text=re.compile(f'CMC.*{date:%Y%m%d%H}.*.grib2'))]
        for f in files_list:
            match = re.match(parse_type,f)
            if match:
                if match[2] == 'ISBL':
                    var = VARNAME[match[2]].get(match[1],match[1])
                    res_dict[f'{var}_P{int(match[3])}-{match[6]}'] = os.path.join(url,f)
                elif match[2] in ['SFC','NTAT']:
                    var = VARNAME[match[2]].get(match[1],match[1])
                    res_dict[f'{var}_L0-{match[6]}'] = os.path.join(url,f)
                elif match[2] in ['MSL']:
                    res_dict[f'{match[1]}_S0-{match[6]}'] = os.path.join(url, f)
                elif match[2] in ['TGL']:
                    var = VARNAME[match[2]].get(match[1],match[1])
                    height = int(match[3] if match[3].isdigit() else match[3][:-1])
                    if height != 2:
                        res_dict[f'{var}_M{height}-{match[6]}'] = os.path.join(url,f)
                    else:
                        res_dict[f'{var}_L0-{match[6]}'] = os.path.join(url,f)
                else:
                    res_dict[f'{match[1]}_{match[2]}{match[3].upper()}-{match[6]}'] = os.path.join(url, f)

    return res_dict


def verify_grib(local_fp:str)->bool:
    """ verify local_grib_file all messages complete or not

    Parameters:
        local_fp: str, local grib filepath
    return
        bool, True if complete else False
    """
    verify = False
    try:
        pygrib.index(local_fp,'shortName')
        verify = True
    except:
        pass
    return verify

@retry(wait_fixed=10E3, stop_max_attempt_number=3)
def single_download(download_url:str, local_fp:str,verify:Callable=verify_grib)->int:
    """ download single file from download url to local path, and verify

    Parameters:
        download_url: str, download url
        local_fp: str, local filepath
        verify: verify function
    return:
        int, the bytes size of file
    """
    session = None
    tmp = local_fp+'.tmp'
    if os.path.exists(local_fp):
        return os.path.getsize(local_fp)
    if os.path.exists(tmp):
        os.remove(tmp)
    if not os.path.exists(os.path.dirname(tmp)):
        os.makedirs(os.path.dirname(tmp),0o777)
    
    try:
        session = requests.Session()
        resp = session.get(download_url,stream=True, timeout=60*5)
        with open(tmp,'wb') as f:
            f.write(resp.content)
            f.flush()
    except Exception as e:
        logger.error(e)
        raise Exception from e
    finally:
        session.close()
    
    if verify(tmp):
        os.rename(tmp,tmp[:-4])
    else:
        os.remove(tmp)
        raise Exception('grib file not complete')
    return os.path.getsize(local_fp)

def batch_download(url_fp_list:list):
    """ download multiply files from download urls to local path

    Parameters:
        url_fp_list: list, [(url, local filepath),...]
    """
    with Pool(PARALLEL_NUM) as pool:
        for i in url_fp_list:
            pool.apply(single_download,args=i)
    pool.join()

def save_geps_ens(date:datetime,batch:int,local_dir:str):
    hours = HOURS['subseason'] if date.weekday() and batch == 0 in [1,4] else HOURS['medium']
    retry_flag = False
    for hour in hours:
        urls = get_files_dict(date, batch,hour)
        # url_fp_list = [(v,os.path.join(local_dir,os.path.basename(v))) for k,v in urls.items()]
        url_fp_list = [(v,os.path.join(local_dir,f'{k}.grib2')) for k,v in urls.items()]
        try:
            batch_download(url_fp_list)
            logger.info(f'GEPS_ENS [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)} HOUR: {hour}] FINISH')
        except:
            retry_flag = True

    if retry_flag:
        return -1

def daily_geps_ens():
    now = datetime.utcnow()
    batch = int((now.hour-7)/12)*12
    HOME = os.environ.get('HOME')
    local_dir = os.environ.get('GEPS_ENS_DIR',now.strftime(f'{HOME}/Downloads/GEPS_ENS/%Y%m%d{str(batch).zfill(2)}00'))
    save_geps_ens(now,batch,local_dir)



def preprocess_single_file(local_fp:str):
    # TODO
    # call(f"cdo -f nc copy {local_fp} {local_fp.replace('.grib2','.nc')}",shell=True)
    # call(f"grib_filter ")
    # fn = os.path.basename(local_fp)
    # pattern = '([A-Z]+)_([A-Za-z0-9]+)-([0-9]+).grib2'
    # match = re.match(pattern,fn)
    # var = fn.split('.grib2')[0].split('_')[0]
    return

def test():
    text = get_files_dict(datetime(2022,5,16),0,3)
    key = 'WIND_M10-003'
    single_download(text[key],f'/Users/blizhan/Downloads/{key}.grib2')
    preprocess_single_file(f'/Users/blizhan/Downloads/{key}.grib2')
    print(text)

if __name__ == "__main__":
    daily_geps_ens()
