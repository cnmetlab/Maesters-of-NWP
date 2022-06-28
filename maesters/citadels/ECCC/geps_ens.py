from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor,as_completed
from subprocess import call,check_output
from datetime import datetime, timedelta
import requests
import re
import os
import sys
import shutil

from glob import glob
from retrying import retry
from bs4 import BeautifulSoup
from loguru import logger
import pygrib

MAESTERS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(MAESTERS)
from config import ECCC_GEPS_ENS, V, PATH
sys.path.append(PATH)

logger.add(os.path.join(os.path.dirname(MAESTERS),'log/GEPS_ENS_{time:%Y%m%d}'),rotation='00:00',retention='3 days')

PARALLEL_NUM = 3

# GEPS_ENS_URL: 'https://dd.weather.gc.ca/ensemble/geps/grib2/{PRODUCT}/{batch}/{hour}/'
# FN EX: 'CMC_geps-raw_AFRAIN_SFC_0_latlon0p5x0p5_2022051400_P192_allmbrs.grib2'

HOURS = {
    'subseason': list(range(3,192,3))+list(range(192,768+6,6)),
    'medium': list(range(3,192,3))+list(range(192,384+6,6))
}


def parse_filename(fn,product:str='raw'):
    prod = 'prob'if product == 'products' else 'raw'
    parse_pattern = f'CMC_geps-{prod}_([A-Z]+)_([A-Z]+)_([0-9A-Za-z]+)_([0-9A-Za-z]+)_([0-9]+)_P([0-9]+)_*'
    match = re.match(parse_pattern, fn)
    if match:
        return match


def get_files_dict(date:datetime,batch:int,hour:int,product:str='raw'):
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
    url = ECCC_GEPS_ENS.download_url.format(PRODUCT=product,batch=batch,hour=hour)
    download_variables_set = ECCC_GEPS_ENS.variable
    resp = requests.get(url)
    res_dict = {}
    if resp.status_code == 200:
        bs_items= BeautifulSoup(resp.text, 'html.parser')
        files_list = [i['href'] for i in bs_items.find_all('a', text=re.compile(f'CMC.*{date:%Y%m%d%H}.*.grib2'))]
        for f in files_list:
            match = parse_filename(f,product=product)
            if match:
                v = V(match[1],match[2],match[3])
                o = download_variables_set.get(v)
                if o:
                    res_dict[f'{o.outname}-{match[6]}'] = os.path.join(url,f)
    return res_dict


@retry(wait_fixed=10E3, stop_max_attempt_number=3)
def single_download(download_url:str, local_fp:str,file_type:str='grib')->int:
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
        logger.error(download_url)
        logger.error(e)
        raise Exception from e
    finally:
        session.close()
    
    if 'grib' in file_type.lower():
        try:
            pygrib.index(tmp)
            os.rename(tmp,tmp[:-4])
        except Exception as e:
            os.remove(tmp)
            logger.error(e)
            raise Exception from e
    elif 'nc' in file_type.lower():
        import xarray as xr
        try:
            with xr.open_dataset(tmp):
                pass
            os.rename(tmp,tmp[:-4])
        except Exception as e:
            os.remove(tmp)
            logger.error(e)
            raise Exception from e
    else:
        try:
            os.rename(tmp,tmp[:-4])
        except Exception as e:
            os.remove(tmp)
            logger.error(e)
            raise Exception from e
    return

def batch_download(url_fp_list:list):
    """ download multiply files from download urls to local path

    Parameters:
        url_fp_list: list, [(url, local filepath),...]
    return:
        fail: list
    """
    futures = []
    fail = []
    with ThreadPoolExecutor(5) as pool:
        for i in url_fp_list:
            futures.append(pool.submit(single_download,download_url=i[0],local_fp=i[1]))
        for n,f in enumerate(as_completed(futures)):
            try:
                f.result()
            except Exception as e:
                fail.append(url_fp_list[n])
    return fail



@retry(wait_fixed=10E3, stop_max_attempt_number=3)
def single_ens_mean(orig_grib_fp:str, out_nc_fp:str,varname:str,split_rule:str=os.path.join(MAESTERS,'static/pf_split')):
    """ cal the mean of all pf type ensemble from grib and save as nc

    Parameters:
        orig_grib_fp: str, original grib filepath
        out_nc_fp: str, output nc filepath
        varname: str, variable name in output nc file
        split_rule: str, the rule_file of split grib, default is pertubationNumber split
    return:
        out_nc_fp
    """
    if os.path.exists(out_nc_fp):
        return out_nc_fp
    orig_dir = os.path.dirname(orig_grib_fp)
    orig_fn = os.path.basename(orig_grib_fp)
    orig_base_filename = orig_fn.split('.grib')[0] if '.grib' in orig_fn else orig_fn.split('.grb')[0]

    out_dir = os.path.dirname(out_nc_fp)
    tmp_dir = os.path.join(orig_dir,f'{orig_base_filename}')
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir,0o777,exist_ok=True)
    call(f"grib_filter {split_rule} ../{orig_fn}",shell=True,cwd=tmp_dir)
    call(f"cdo -s ensmean *-*.pn* temp.grib ",cwd=tmp_dir,shell=True)
    paraName = check_output(f"cdo showname temp.grib",cwd=tmp_dir,shell=True).decode('utf-8').split('\n')[0][1:]
    os.makedirs(out_dir,0o777,exist_ok=True)
    call(f"cdo -f nc -chname,{paraName},{varname} temp.grib {out_nc_fp}",cwd=tmp_dir,shell=True)
    shutil.rmtree(tmp_dir)
    return out_nc_fp


def batch_ens_mean(in_out_var_list:list,split_rule:str=os.path.join(MAESTERS,'static/pf_split'))->list:
    """ batch cal ens mean 

    Parameters:
        in_out_var_list: list, [(orig_grib_fp, out_nc_fp, varname), ...,]
        split_rule: str, grib_filter split rule_file
    return:
        list, fail list
    """
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
        for value in in_out_var_list:
            results.append(pool.submit(single_ens_mean,orig_grib_fp=value[0],out_nc_fp=value[1],varname=value[2],split_rule=split_rule))
        for n,r in enumerate(results):
            try:
                r.result()
            except Exception as e:
                logger.error(in_out_var_list[n])
                logger.error(e)
                fail.append(in_out_var_list[n])
    return fail


def save_geps_ens(date:datetime,batch:int,local_dir:str,product='raw'):
    """ save all geps_ens batch files at date in directory

    Parameters:
        date: datetime, UTC
        batch: int, batch hour, 0/12
        local_dir: str, save dir
        product: str, geps_ens product, 'raw'/'products'
    return:
        -1 if some fail
        0 if all success
    """
    logger.info(f'GEPS_ENS {date:%Y%m%d}{str(batch).zfill(2)} download start')
    hours = HOURS['subseason'] if date.weekday() in [3] and batch == 0  else HOURS['medium']
    retry_flag = False
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
        for hour in hours[:]:
            urls = get_files_dict(date, batch, hour, product=product)
            url_fp_list = [(v,os.path.join(local_dir,os.path.basename(v))) for k,v in urls.items()]
            # url_fp_list = [(v,os.path.join(local_dir,f'{k}.grib2')) for k,v in urls.items()]
            results.append(pool.submit(batch_download,url_fp_list=url_fp_list))
        for n,r in enumerate(results):
            res = r.result()
            if res:
                fail.extend(res)
            else:
                logger.info(f'GEPS_ENS: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)} HOUR: {hours[n]}] DOWNLOAD FINISH')

    fail = batch_download(fail)
    if fail:
        logger.error('the following download fail')
        logger.error(fail)
        return -1
    else:
        logger.info(f'GEPS_ENS: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)}] ALL DOWNLOAD FINISH')
        return 0


def geps_ens_mean(grib_dir:str,out_dir:str,split_rule:str=os.path.join(MAESTERS,'static/pf_split')):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir,0o777,exist_ok=True)
    files = glob(os.path.join(grib_dir,'CMC*.grib*'))
    fns = [os.path.basename(f) for f in files]
    matches = [parse_filename(fn) for fn in fns]
    in_out_var_list = []
    for n,m in enumerate(matches):
        if m:
            v = V(m[1],m[2],m[3])
            o = ECCC_GEPS_ENS.variable.get(v)
            if o:
                t = (files[n],os.path.join(out_dir,f'{o.outname}-{m[6]}.nc'),o.outname)
                in_out_var_list.append(t)
    fail = batch_ens_mean(in_out_var_list,split_rule)
    
    fail = batch_ens_mean(fail,split_rule)
    if fail:
        logger.error('the following cal ens-mean fail')
        logger.error(fail)
        return -1
    else:
        logger.info(f'GEPS_ENS: ALL ENS_MEAN CALC FINISH')
        return 0


def daily_geps_ens():
    now = datetime.utcnow() - timedelta(hours=6)
    batch = int(now.hour/12)*12
    orig_dir = now.strftime(os.path.join(ECCC_GEPS_ENS.data_dir, f'%Y%m%d{str(batch).zfill(2)}0000'))
    archive_dir = now.strftime(os.path.join(ECCC_GEPS_ENS.archive_dir, f'%Y%m%d{str(batch).zfill(2)}0000'))
    save_geps_ens(now,batch,orig_dir,'raw')
    geps_ens_mean(orig_dir,archive_dir)


if __name__ == "__main__":
    daily_geps_ens()
