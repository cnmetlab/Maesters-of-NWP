import re
import os
import sys
from datetime import datetime, timedelta
import shutil
from glob import glob
from concurrent.futures import ProcessPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from loguru import logger
from retrying import retry

from maesters.config import get_model, V
from maesters.utils.download import batch_session_download, single_session_download
from maesters.utils.post_process import batch_tri_transform, single_tri_transform

MAESTERS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger.add(
    os.path.join(os.path.dirname(MAESTERS), "log/DWD_ICON_{time:%Y%m%d}"),
    rotation="00:00",
    retention=10,
)

PARALLEL_NUM = 5

DWD_ICON = get_model("dwd", "icon")
HOURS = {"medium": (list(range(0, 78, 1)) + list(range(78, 180 + 3, 3)))}


def parse_filename(fn: str):
    """parse filename return match result

    Args:
        fn (str): filename

    Returns:
        _type_: _description_
    """
    parse_pattern = (
        r"icon_global_[a-z]+_([a-z]+)-[a-z]+_([0-9]+)_([0-9]+)_([0-9A-Z\_]+).grib2*"
    )
    match = re.match(parse_pattern, fn)
    return match


def get_files_dict(
    date: datetime, hour: int = None, var_dict: dict = DWD_ICON.variable
):
    """get newest DWD Files at batch hour of var_dict
    Parameters:
        batch: int, batch_hour 0/6/12/18
        var_dict: dict {V(varname, level_type, level),O}
    return:
        dict
            {
                "{VARNAME}_{LEVEL}-{HOUR}":"{'url': URL}"
            }
    """
    batch = str(date.hour).zfill(2)
    res_dict = {}
    for variable, out in var_dict.items():
        url = os.path.dirname(DWD_ICON.download_url).format(
            batch=batch, variable=variable.varname.lower()
        )
        resp = requests.get(url)
        if resp.status_code == 200:
            bs_items = BeautifulSoup(resp.text, "html.parser")
            files_list = [
                i["href"]
                for i in bs_items.find_all("a", text=re.compile("icon_global_*"))
            ]
            for f in files_list:
                match = parse_filename(f)
                if isinstance(hour, int):
                    hours = [hour]
                else:
                    hours = HOURS["medium"]
                if match and int(match[3]) in hours:  # HOURS['medium']:
                    level_type = match[1]
                    levels = re.findall(r"\d+", match[4])
                    level = levels[0] if len(levels) else "0"
                    if level_type == "single":
                        varname = "".join(
                            re.findall(r"^[A-Z\d]+|\_[\dA-Z]+|[^\d\_]", match[4])
                        )
                    else:
                        varname = "".join(
                            re.findall(r"[A-Z]+\_[\dA-Z]+|[^\d\_]+$", match[4])
                        )
                    if V(varname, level_type, level) == variable:
                        res_dict[
                            f"{DWD_ICON.variable.get(variable).outname}-{match[3]}"
                        ] = {"url": os.path.join(url, f)}
        else:
            raise Exception(f"url: {url} not exists")
    return res_dict


def download(**kwargs):
    """single download and transfrom to unit format

    Returns:
        kwargs: url: str, local_fp: str
    """
    single_session_download(
        download_url=kwargs["url"],
        local_fp=kwargs["local_fp"].replace(".nc", ".grib2"),
        file_type="bz2",
    )
    single_tri_transform(
        kwargs["local_fp"].replace(".nc", ".grib2"),
        kwargs["local_fp"],
        os.path.basename(kwargs["local_fp"]).split("-")[0],
    )
    os.remove(kwargs["local_fp"].replace(".nc", ".grib2"))
    return os.path.getsize(kwargs["local_fp"])


def save_dwd_icon(date: datetime, local_dir: str):
    """save all geps_ens batch files at date in directory

    Parameters:
        date: datetime, UTC
        local_dir: str, save dir
    return:
        -1 if some fail
        0 if all success
    """
    batch = date.hour
    logger.info(f"DWD_ICON {date:%Y%m%d}{str(batch).zfill(2)} download start")
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:

        for k, v in DWD_ICON.variable.items():
            urls = get_files_dict(date.replace(hour=batch), var_dict={k: v})
            url_fp_list = [
                (
                    v.get("url"),
                    os.path.join(
                        local_dir, os.path.basename(v.get("url")).split(".bz2")[0]
                    ),
                )
                for k, v in urls.items()
            ]
            results.append(
                pool.submit(
                    batch_session_download, url_fp_list=url_fp_list, file_type="bz2"
                )
            )
        for n, r in enumerate(as_completed(results)):
            res = r.result()
            if res:
                fail.extend(res)
            else:
                logger.info(
                    f"DWD_ICON: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)} "
                    f"VARNAME: {list(DWD_ICON.variable.keys())[n].varname}] DOWNLOAD FINISH"
                )

    fail = batch_session_download(fail, file_type="bz2")
    if fail:
        logger.error("the following download fail")
        logger.error(fail)
        return -1
    else:
        logger.info(
            f"DWD_ICON: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)}] ALL DOWNLOAD FINISH"
        )
        return 0


def dwd_transform(
    grib_dir: str,
    out_dir: str,
    grid_text: str = os.path.join(MAESTERS, "static/dwd/target_grid_world_0125.txt"),
    grid_weight: str = os.path.join(MAESTERS, "static/dwd/weights_icogl2world_0125.nc"),
) -> int:
    """transfrom all files in grib dir to lon-lat-grid nc

    Parameters:
        grib_dir: str, grib file directory
        out_dir: str, out file directory
        grid_text: str, transform output grid details file
        grid_weight: str, transform orig grid weight file

    """
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, 0o777, exist_ok=True)
    files = glob(os.path.join(grib_dir, "icon*.grib2"))
    fns = [os.path.basename(f) for f in files]
    matches = [parse_filename(fn) for fn in fns]
    in_out_var_list = []
    for n, m in enumerate(matches):
        if m:
            level_type = m[1]
            levels = re.findall(r"\d+", m[4])
            level = levels[0] if len(levels) else "0"
            varname = re.findall(r"^[A-Z]+\_[\dA-Z]+|[^\d\_]", m[4])[0]
            hour = m[3]
            if level and level_type and hour:
                v = V(varname, level_type, level)
                o = DWD_ICON.variable.get(v)
                if o:
                    t = (
                        files[n],
                        os.path.join(out_dir, f"{o.outname}-{hour}.nc"),
                        o.outname,
                    )
                    in_out_var_list.append(t)
    fail = batch_tri_transform(in_out_var_list, grid_text, grid_weight)
    fail = batch_tri_transform(fail, grid_text, grid_weight)
    if fail:
        logger.error("the following tri-transform fail")
        logger.error(fail)
        return -1
    else:
        logger.info("DWD_ICON: ALL TRI-TRANSFORM FINISH")
        return 0


@retry(stop_max_delay=3 * 60 * 60 * 10e3, stop_max_attempt_number=1)
def operation(local_dir: str = None):
    now = datetime.utcnow() - timedelta(hours=4)
    batch = int(now.hour / 12) * 12
    tmp_dir = (
        now.strftime(
            os.path.join(
                DWD_ICON.data_dir.replace("~", os.environ.get("HOME")) + "_tmp",
                f"%Y%m%d{str(batch).zfill(2)}0000",
            )
        )
        if local_dir is None
        else local_dir + "_tmp"
    )
    local_dir = (
        now.strftime(
            os.path.join(
                DWD_ICON.data_dir.replace("~", os.environ.get("HOME")),
                f"%Y%m%d{str(batch).zfill(2)}0000",
            )
        )
        if local_dir is None
        else local_dir
    )
    save_dwd_icon(now.replace(hour=batch), tmp_dir)
    dwd_transform(tmp_dir, local_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        operation()
    else:
        operation(sys.argv[1])
    print()
