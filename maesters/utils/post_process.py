from subprocess import call,check_output
import os,sys
from concurrent.futures import ProcessPoolExecutor,as_completed
import shutil

from loguru import logger
from retrying import retry

try:
    PATH = os.path.dirname(check_output('which cdo',shell=True).decode('utf-8').split('\n')[0][:])
except:
    PATH = os.path.dirname(sys.executable)
MAESTERS = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARALLEL_NUM = 5

@retry(wait_fixed=10E3, stop_max_attempt_number=3,stop_max_delay=10*10E3)
def single_convert_nc(orig_grib_fp:str, out_nc_fp:str):
    """ change name and convert grib to nc

    Parameters:
        orig_grib_fp: str, original grib filepath
        out_nc_fp: str, output nc filepath
    return:
        out_nc_fp
    """
    if os.path.exists(out_nc_fp):
        return out_nc_fp
    varname = os.path.basename(out_nc_fp).split('.nc')[0].split('-')[0]
    out_dir = os.path.dirname(out_nc_fp)
    os.makedirs(out_dir,0o777,exist_ok=True)
    call(f"{os.path.join(PATH,'cdo')} showname {orig_grib_fp} | xargs -I {'{}'} {os.path.join(PATH,'cdo')} -f nc4 -chname,{'{}'},{varname} {orig_grib_fp} {out_nc_fp}",shell=True)
    os.chmod(out_nc_fp,0o777)
    return out_nc_fp


def batch_convert_nc(in_out_list:list)->list:
    """ batch rename and convert grib to nc

    Parameters:
        in_out_list: list, [(orig_grib_fp, out_nc_fp, varname), ...,]
    return:
        list, fail list
    """
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
        for value in in_out_list:
            results.append(pool.submit(single_convert_nc,orig_grib_fp=value[0],out_nc_fp=value[1]))
        for n,r in enumerate(as_completed(results)):
            try:
                r.result()
            except Exception as e:
                logger.error(in_out_list[n])
                logger.error(e)
                fail.append(in_out_list[n])
    return fail


@retry(wait_fixed=10E3, stop_max_attempt_number=3,stop_max_delay=10*10E3)
def single_tri_transform(orig_grib_fp:str,out_nc_fp:str,varname:str,grid_text:str=os.path.join(MAESTERS,'static/dwd/target_grid_world_0125.txt'),\
    grid_weight:str=os.path.join(MAESTERS,'static/dwd/weights_icogl2world_0125.nc')):
    """ single transform: 1. dwd icon tri-grid to lon-lat grid 2. grib to nc 3. change variable name

    Parameters:
        orig_grib_fp: str, orig grib filepath
        out_nc_fp: str, out nc filepath
        varname: rename variable name
        grid_text: str, transform output grid details file
        grid_weight: str, transform orig grid weight file
    """
    orig_dir = os.path.dirname(orig_grib_fp)
    orig_fn = os.path.basename(orig_grib_fp)
    out_dir = os.path.dirname(out_nc_fp)

    if os.path.exists(out_nc_fp):
        return
    if os.path.exists(os.path.join(out_dir, orig_fn+'.tmp')):
        os.remove(os.path.join(out_dir, orig_fn+'.tmp'))
    os.makedirs(out_dir,0o777,exist_ok=True)

    call(f"{os.path.join(PATH,'cdo')} -f grb2 remap,{grid_text},{grid_weight} {orig_grib_fp} {orig_fn+'.tmp'}",cwd=out_dir,shell=True)
    # paraName = check_output(f"{os.path.join(PATH,'cdo')} showname {orig_fn}",cwd=out_dir,shell=True).decode('utf-8').split('\n')[0][1:]
    call(f"{os.path.join(PATH,'cdo')} showname {orig_fn+'.tmp'} | xargs -I {'{}'} {os.path.join(PATH,'cdo')} -f nc -chname,{'{}'},{varname} {orig_fn+'.tmp'} {out_nc_fp}",cwd=out_dir,shell=True)
    os.remove(os.path.join(out_dir, orig_fn+'.tmp'))

    return


def batch_tri_transform(in_out_var_list:list,grid_text:str=os.path.join(MAESTERS,'static/dwd/target_grid_world_0125.txt'),\
    grid_weight:str=os.path.join(MAESTERS,'static/dwd/weights_icogl2world_0125.nc'))->list:
    """ batch transform dwd icon tri-grid grib data to lon-lat-grid nc

    Parameters:
        in_out_var_list: list, [(orig_grib_fp, out_nc_fp, varname), ...,]
        grid_text: str, transform output grid details file
        grid_weight: str, transform orig grid weight file
    Return:
        list, fail list
    """
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
        for value in in_out_var_list:
            results.append(pool.submit(single_tri_transform,orig_grib_fp=value[0],out_nc_fp=value[1],varname=value[2],\
                grid_text=grid_text,grid_weight=grid_weight))
        for n,r in enumerate(as_completed(results)):
            try:
                r.result()
            except Exception as e:
                logger.error(in_out_var_list[n])
                logger.error(e)
                fail.append(in_out_var_list[n])
    return fail


@retry(wait_fixed=10E3, stop_max_attempt_number=3,stop_max_delay=10*10E3)
def single_ens_stats(orig_grib_fp:str, out_nc_fp:str,varname:str,stats:str,split_rule:str=os.path.join(MAESTERS,'static/pf_split')):
    """ cal the ens method of all pf type ensemble from grib and save as nc

    Parameters:
        orig_grib_fp: str, original grib filepath
        out_nc_fp: str, output nc filepath
        varname: str, variable name in output nc file
        stats: str, ens method like 'ensmean'/'ensmax'/'ensmin'/'ensstd'/'ensstd1'/'enssum'/'ensvar'/'ensvar1'/'ensskew'/'enspctl'/'ensmedian'/'enskurt'/'ensrange'
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
    call(f"{os.path.join(PATH,'grib_filter')} {split_rule} ../{orig_fn}",shell=True,cwd=tmp_dir)
    call(f"{os.path.join(PATH,'cdo')} -s {stats} *-*.pn* temp.grib ",cwd=tmp_dir,shell=True)
    os.makedirs(out_dir,0o777,exist_ok=True)
    call(f"{os.path.join(PATH,'cdo')} showname temp.grib | xargs -I {'{}'} {os.path.join(PATH,'cdo')} -f nc -chname,{'{}'},{varname} temp.grib {out_nc_fp}",cwd=tmp_dir,shell=True)
    shutil.rmtree(tmp_dir)
    return out_nc_fp


@retry(wait_fixed=10E3, stop_max_attempt_number=3,stop_max_delay=10*10E3)
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
    return single_ens_stats(orig_grib_fp,out_nc_fp,varname,'ensmean',split_rule)

def batch_ens_stats(in_out_var_list:list,stats:str,split_rule:str=os.path.join(MAESTERS,'static/pf_split'))->list:
    """ batch cal ens stats 

    Parameters:
        in_out_var_list: list, [(orig_grib_fp, out_nc_fp, varname), ...,]
        stats: str, ens method like 'ensmean'/'ensmax'/'ensmin'/'ensstd'/'ensstd1'/'enssum'/'ensvar'/'ensvar1'/'ensskew'/'enspctl'/'ensmedian'/'enskurt'/'ensrange'
        split_rule: str, grib_filter split rule_file
    return:
        list, fail list
    """
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
        for value in in_out_var_list:
            results.append(pool.submit(single_ens_stats,orig_grib_fp=value[0],out_nc_fp=value[1],varname=value[2],stats=stats,split_rule=split_rule))
        for n,r in enumerate(as_completed(results)):
            try:
                r.result()
            except Exception as e:
                logger.error(in_out_var_list[n])
                logger.error(e)
                fail.append(in_out_var_list[n])
    return fail

def batch_ens_mean(in_out_var_list:list,split_rule:str=os.path.join(MAESTERS,'static/pf_split'))->list:
    """ batch cal ens mean 

    Parameters:
        in_out_var_list: list, [(orig_grib_fp, out_nc_fp, varname), ...,]
        split_rule: str, grib_filter split rule_file
    return:
        list, fail list
    """
    return batch_ens_stats(in_out_var_list,'ensmean',split_rule=split_rule)

# def batch_ens_mean(in_out_var_list:list,split_rule:str=os.path.join(MAESTERS,'static/pf_split'))->list:
#     """ batch cal ens mean 

#     Parameters:
#         in_out_var_list: list, [(orig_grib_fp, out_nc_fp, varname), ...,]
#         split_rule: str, grib_filter split rule_file
#     return:
#         list, fail list
#     """
#     results = []
#     fail = []
#     with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
#         for value in in_out_var_list:
#             results.append(pool.submit(single_ens_mean,orig_grib_fp=value[0],out_nc_fp=value[1],varname=value[2],split_rule=split_rule))
#         for n,r in enumerate(as_completed(results)):
#             try:
#                 r.result()
#             except Exception as e:
#                 logger.error(in_out_var_list[n])
#                 logger.error(e)
#                 fail.append(in_out_var_list[n])
#     return fail
