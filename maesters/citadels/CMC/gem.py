import shutil
from retrying import retry
from bs4 import BeautifulSoup
from loguru import logger
# import pygrib

from glob import glob
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor,as_completed
# from subprocess import call,check_output
from datetime import datetime, timedelta
import requests
import re
import os
import sys
# import shutil

MAESTERS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(MAESTERS)
from config import V,get_model
from utils.download import batch_session_download,single_session_download
from utils.post_process import batch_ens_mean,single_ens_mean,batch_convert_nc,single_convert_nc

logger.add(os.path.join(os.path.dirname(MAESTERS),'log/GEM_{time:%Y%m%d}'),rotation='00:00',retention=10)

PARALLEL_NUM = 5

CMC_GEM = get_model('cmc','gem')
HOURS = {
    'medium': list(range(3,240,3))
}


def parse_filename(fn):
    'CMC_geps-{TYPE}_{var}_{level_type}_{level}_latlon0p5x0p5_%Y%m%d%H_P{hour}_allmbrs.grib2'
    parse_pattern = 'CMC_glb_([A-Z]+)_([A-Z]+)_([0-9A-Za-z]+)_([0-9a-zA-Z\.]+)_([0-9]+)_P([0-9]+).grib2'
    # parse_pattern = f'CMC_geps-{prod}_([A-Z]+)_([A-Z]+)_([0-9A-Za-z]+)_([0-9A-Za-z]+)_([0-9]+)_P([0-9]+)_*'
    match = re.match(parse_pattern, fn)
    if match:
        return match


def get_files_dict(date:datetime,hour:int,var_dict:dict=CMC_GEM.variable):
    """ get GEM Files at date/batch/hour

    Parameters:
        date: datetime, initial datetime (UTC)
        hour: int, step hour
        var_dict: dict, variable_dict
    return:
        dict
            {
                "{VARNAME}_{LEVEL}-{HOUR}":"{'url':URL}"
            }
    """
 
    batch = str(date.hour).zfill(2)
    hour = str(hour).zfill(3)
    url = os.path.dirname(CMC_GEM.download_url).format(batch=batch,hour=hour)
    resp = requests.get(url)
    res_dict = {}
    if resp.status_code == 200:
        bs_items= BeautifulSoup(resp.text, 'html.parser')
        files_list = [i['href'] for i in bs_items.find_all('a', text=re.compile(f'CMC.*{date:%Y%m%d%H}.*.grib2'))]
        for f in files_list:
            match = parse_filename(f)
            if match:
                v = V(match[1],match[2],match[3])
                o = var_dict.get(v)
                if o:
                    res_dict[f'{o.outname}-{match[6]}'] = {'url':os.path.join(url,f)}
    return res_dict

def download(**kwargs):
    single_session_download(download_url=kwargs['url'],local_fp=kwargs['local_fp'].replace('.nc','.grib2'))
    single_convert_nc(kwargs['local_fp'].replace('.nc','.grib2'),kwargs['local_fp'])
    os.remove(kwargs['local_fp'].replace('.nc','.grib2'))
    return os.path.getsize(kwargs['local_fp'])



def save_cmc_gem(date:datetime,local_dir:str):
    """ save all gem batch files at date in directory

    Parameters:
        date: datetime, UTC
        local_dir: str, save dir
    return:
        -1 if some fail
        0 if all success
    """
    batch = date.hour
    logger.info(f'GEM {date:%Y%m%d}{str(batch).zfill(2)} download start')
    hours = HOURS['medium']
    retry_flag = False
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
        for hour in hours[:]:
            urls = get_files_dict(date, hour)
            url_fp_list = [(v['url'],os.path.join(local_dir,os.path.basename(v['url']))) for k,v in urls.items()]
            # url_fp_list = [(v,os.path.join(local_dir,f'{k}.grib2')) for k,v in urls.items()]
            results.append(pool.submit(batch_session_download,url_fp_list=url_fp_list))
        for n,r in enumerate(as_completed(results)):
            res = r.result()
            if res:
                fail.extend(res)
            else:
                logger.info(f'GEM: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)} HOUR: {hours[n]}] DOWNLOAD FINISH')

    fail = batch_session_download(fail)
    if fail:
        logger.error('the following download fail')
        logger.error(fail)
        return -1
    else:
        logger.info(f'GEM: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)}] ALL DOWNLOAD FINISH')
        return 0


def convert_cmc_gem(grib_dir:str,out_dir:str):
    grib_files = glob(os.path.join(grib_dir,'*.grib*'))
    in_out_list = [(f, os.path.join(out_dir,os.path.basename(f).split('.gr')[0]+'.nc')) for f in grib_files]
    fail = batch_convert_nc(in_out_list)
    fail = batch_convert_nc(fail)
    if fail:
        logger.error('the following convern nc fail')
        logger.error(fail)
        return -1
    else:
        logger.info(f'CMC_GEM: ALL CONVERT FINISH')
        return 0


@retry(stop_max_delay=3*60*60*10E3,stop_max_attempt_number=1)
def operation(data_dir:str=None):
    now = datetime.utcnow() - timedelta(hours=4)
    batch = int(now.hour/12)*12
    tmp_dir = now.strftime(os.path.join(CMC_GEM.data_dir.replace('~',os.environ.get('HOME'))+'_tmp', \
        f'%Y%m%d{str(batch).zfill(2)}0000')) if data_dir is None else data_dir+'_tmp'
    data_dir = now.strftime(os.path.join(CMC_GEM.data_dir.replace('~',os.environ.get('HOME')), \
        f'%Y%m%d{str(batch).zfill(2)}0000')) if data_dir is None else data_dir
    save_cmc_gem(now.replace(hour=batch),tmp_dir)
    convert_cmc_gem(tmp_dir,data_dir)
    shutil.rmtree(tmp_dir,ignore_errors=True)

if __name__ == "__main__":
    if len(sys.argv)<=1:
        operation()
    else:
        operation(sys.argv[1])
