from typing import Callable
from retrying import retry
import requests
import pygrib
from loguru import logger

import http.cookiejar
import http.client
import urllib
import os
from concurrent.futures import ThreadPoolExecutor,as_completed
import re
import time
import shutil
import bz2

@retry(wait_fixed=5E3, stop_max_attempt_number=3)
def decompress_check_grib(bz_fp:str,de_fp:str):
    """ 1. decompress, 2. check grib and 3. remove orig bz

    Parameters:
        bz_fp: bz2 filepath
        de_fp: decompressed filepath
    Return:
        0 if success else raise Exception
    """
    with open(de_fp,'wb') as new_file,bz2.BZ2File(bz_fp,'rb') as file:
        for data in iter(lambda: file.read(100 * 1024), b''):
            new_file.write(data)
            new_file.flush()
    pygrib.index(de_fp,'shortName')
    os.remove(bz_fp)
    return 0


def before_download(local_fp:str):
    """ precosse before download. 
            1. if previous then remove previous file
            2. makedirs
    
    Parameters:
        local_fp: str
    """
    if os.path.exists(local_fp):
        os.remove(local_fp)
    if not os.path.exists(os.path.dirname(local_fp)):
        os.makedirs(os.path.dirname(local_fp),0o777)


def after_download(local_fp:str, file_type:str,rename_fp:str=None):
    """ process after download
            1. check grib and nc
            2. rename
    
    Parameters:
        local_fp: str, origin local filepath
        file_type: str, file type
        rename_fp: str, rename filepath
    """


    if 'grib' in file_type.lower():
        try:
            pygrib.index(local_fp)
            if rename_fp:
                # os.rename(local_fp,rename_fp)
                shutil.move(local_fp,rename_fp)
        except Exception as e:
            os.remove(local_fp)
            raise Exception from e
    elif 'nc' in file_type.lower():
        import xarray as xr
        try:
            with xr.open_dataset(local_fp):
                pass
            if rename_fp:
                # os.rename(local_fp,rename_fp)
                shutil.move(local_fp,rename_fp)

        except Exception as e:
            os.remove(local_fp)
            raise Exception from e

    elif 'bz2' in file_type.lower():
        try:
            decompress_check_grib(local_fp,rename_fp)
        except Exception as e:
            os.remove(local_fp)
            raise Exception from e
    else:
        try:
            if rename_fp:
                # os.rename(local_fp,rename_fp)
                shutil.move(local_fp,rename_fp)

        except Exception as e:
            os.remove(local_fp)
            raise Exception from e


@retry(wait_fixed=10E3, stop_max_attempt_number=3)
def single_session_download(download_url:str, local_fp:str,file_type:str='grib')->int:
    """ download single file from download url to local path using session, and verify

    Parameters:
        download_url: str, download url
        local_fp: str, local filepath
        file_type: str, grib 
    return:
        int, the bytes size of file
    """
    session = None
    tmp = local_fp+'.tmp'
    if os.path.exists(local_fp):
        return os.path.getsize(local_fp)

    before_download(tmp)

    try:
        session = requests.Session()
        resp = session.get(download_url,stream=True, timeout=60*5)
        with open(tmp,'wb') as f:
            f.write(resp.content)
            f.flush()
        session.close()
    except Exception as e:
        os.remove(tmp)
        session.close()
        raise Exception from e

    after_download(tmp,file_type,local_fp)
    return os.path.getsize(local_fp)

@retry(wait_fixed=10E3,stop_max_attempt_number=3)
def single_range_download(download_url:str,start_bytes:str,end_bytes:str, local_fp:str,file_type:str='grib')->int:
    """ download bytes range of single file from download url to local path using curl command

    Parameters:
        download_url: str, download url
        start_bytes: str, the download_range bytes start
        end_bytes: str, the download_range bytes end
        local_fp: str, local filepath
        file_type: str, grib 
    return:
        int, the bytes size of file
    """
    tmp = local_fp+'.tmp'
    if os.path.exists(local_fp):
        return os.path.getsize(local_fp)
    
    before_download(tmp)
    try:
        cmd = f'curl -s --range {start_bytes}-{end_bytes} {download_url} > {tmp}'
        os.system(cmd)
    except Exception as e:
        os.remove(tmp)
        raise Exception from e
    
    after_download(tmp,file_type,local_fp)
    return os.path.getsize(local_fp)


def batch_session_download(url_fp_list:list,file_type:str='grib',download_func:Callable=single_session_download,thread_num:int=5):
    """ download multiply files from download urls to local path using session, and handle logger

    Parameters:
        url_fp_list: list, [(url, local filepath),...]
        file_type: str
        download_func: Callable
        thread_num: int, default is 5
    return:
        fail: list
    """
    futures = []
    fail = []
    with ThreadPoolExecutor(thread_num) as pool:
        for i in url_fp_list:
            futures.append(pool.submit(download_func,download_url=i[0],local_fp=i[1],file_type=file_type))
        for n,f in enumerate(as_completed(futures)):
            # f.result()
            try:
                f.result()
            except Exception as e:
                logger.error(url_fp_list[n][0])
                logger.error(e)
                fail.append(url_fp_list[n])
    return fail

def batch_range_download(inputs_list:list, file_type:str='grib',thread_num:int=5):
    """ range-download multi files from download urls to local path using curl, and handle logger

    Parameters:
        inputs_list: list, [(url, start_bytes, end_bytes, local filepath),...]
        file_type: str
        thread_num: int, default is 5
    return:
        fail: list
    """
    futures = []
    fail = []
    with ThreadPoolExecutor(thread_num) as pool:
        for i in inputs_list:
            futures.append(pool.submit(single_range_download,download_url=i[0], start_bytes=i[1], end_bytes=i[2], local_fp=i[3], file_type=file_type))
        for n,f in enumerate(as_completed(futures)):
            # f.result()
            try:
                f.result()
            except Exception as e:
                logger.error(inputs_list[n][0])
                logger.error(e)
                fail.append(inputs_list[n])
    return fail

class auth_download:
    def __init__(self) -> None:
        self.cj = http.cookiejar.MozillaCookieJar()
        self.handler = urllib.request.HTTPCookieProcessor(self.cj)
        self.opener = urllib.request.build_opener(self.handler)
        pass


    def login(self,login_url:str,head:dict):
        """ login action at url with login dict

        Parameters:
            login_url: str
            login_dict: dict
        
        """
        pattern = 'https?://([^//]+)*/'
        m = re.match(pattern, login_url)
        auth_file = f'auth.{m.group(1)}' if m else f'auth.{time.time()}'

        do_authentication=False
        if (os.path.isfile(auth_file)):
            self.cj.load(auth_file,False,True)

        for cookie in self.cj:
            if (cookie.name == "sess" and cookie.is_expired()):
                do_authentication=True
        else:
            do_authentication=True

        if (do_authentication):
            urllib.request.Request(login_url,urllib.parse.urlencode(head).encode('utf-8'))
            # login = urllib.request.urlopen(url,urllib.parse.urlencode(head).encode('utf-8'))
            # opener.open("https://rda.ucar.edu/cgi-bin/login",urllib.parse.urlencode(head).encode('utf-8'))
            self.cj.clear_session_cookies()
            self.cj.save(auth_file,True,True)
        return

    def download(self, url):
        idx=url.rfind("/")
        if (idx > 0):
            ofile=url[idx+1:]
        else:
            ofile=url

        if os.path.exists(ofile):
            os.remove(ofile)
        try:
            with open(ofile,'wb') as outfile:
                # infile = urllib.request.urlopen(url)
                infile=self.opener.open(url)
                outfile.write(infile.read())
                outfile.close()
                print(ofile)
        except Exception as e:
            logger.error(e)

    @retry(wait_fixed=10E3, stop_max_attempt_number=3)
    def single_session_auth_download(self, download_url:str, local_fp:str,file_type:str='grib')->int:
        """ download single file from download url to local path using session with auth, and verify

        Parameters:
            download_url: str, download url
            local_fp: str, local filepath
            verify: verify function
        return:
            int, the bytes size of file
        """
        idx=download_url.rfind("/")
        if (idx > 0):
            tmp = os.path.join('.',download_url[idx+1:])
        else:
            tmp = os.path.join('.',download_url)
        tmp = tmp+'.tmp'

        if os.path.exists(local_fp):
            return os.path.getsize(local_fp)
        before_download(tmp)

        try:
            with open(tmp,'wb') as outfile:
                infile=self.opener.open(download_url)
                # content = infile.read()
                # infile = urllib.request.urlopen(download_url)
                try:
                    content = infile.read()
                except http.client.IncompleteRead as e:
                    content = e.partial
                outfile.write(content)
                outfile.flush()
        except Exception as e:
            logger.error(e)
            os.remove(tmp)
            raise Exception from e
        
        after_download(tmp,file_type,local_fp)
        return os.path.getsize(local_fp)

    def batch_download(self, url_fp_list:list, file_type:str='grib')->list:
        """ download multiply files from download urls to local path using session with auth, and handle logger

        Parameters:
            url_fp_list: list, [(url, local filepath),...]
            file_type: str
        return:
            fail: list
        """
        fail = batch_session_download(url_fp_list,file_type,self.single_session_auth_download,1)
        return fail
