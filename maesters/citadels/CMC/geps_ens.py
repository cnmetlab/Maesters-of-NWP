import requests
import re
import os
import sys
from glob import glob
from datetime import datetime, timedelta
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed


from retrying import retry
from bs4 import BeautifulSoup
from loguru import logger

from maesters.utils.post_process import batch_ens_stats
from maesters.config import get_model, V
from maesters.utils.download import batch_session_download, single_session_download
from maesters.utils.post_process import single_ens_mean, single_ens_stats

MAESTERS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger.add(
    os.path.join(os.path.dirname(MAESTERS), "log/GEPS_ENS_{time:%Y%m%d}"),
    rotation="00:00",
    retention=10,
)

PARALLEL_NUM = 5

# GEPS_ENS_URL: 'https://dd.weather.gc.ca/ensemble/geps/grib2/{PRODUCT}/{batch}/{hour}/'
# FN EX: 'CMC_geps-raw_AFRAIN_SFC_0_latlon0p5x0p5_2022051400_P192_allmbrs.grib2'

CMC_GEPS_ENS = get_model("cmc", "geps_ens")
HOURS = {
    "subseason": list(range(3, 192, 3)) + list(range(192, 768 + 6, 6)),
    "medium": list(range(3, 192, 3)) + list(range(192, 384 + 6, 6)),
}


def parse_filename(fn: str, data_type: str = "raw") -> re.Match:
    """parse filename from cmc

    Args:
        fn (str): filename
        data_type (str, optional): data type. Defaults to 'raw'.

    Returns:
        re.Match: match result
    """

    prod = "prob" if data_type == "products" else "raw"
    parse_pattern = f"CMC_geps-{prod}_([A-Z]+)_([A-Z]+)_([0-9A-Za-z]+)_([0-9A-Za-z]+)_([0-9]+)_P([0-9]+)_*"
    match = re.match(parse_pattern, fn)
    if match:
        return match


def get_files_dict(
    date: datetime,
    hour: int,
    var_dict: dict = CMC_GEPS_ENS.variable,
    data_type: str = "raw",
):
    """get GEPS_ENS Files at date/batch/hour

    Parameters:
        date: datetime, initial datetime (UTC)
        hour: int, step hour
        var_dict: dict, variable_dict
        data_type: str, data_type (raw/product)
    return:
        dict
            {
                "{VARNAME}_{LEVEL}-{HOUR}":"{'url':URL}"
            }
    """

    batch = str(date.hour).zfill(2)
    hour = str(hour).zfill(3)
    url = os.path.dirname(CMC_GEPS_ENS.download_url).format(
        PRODUCT=data_type,
        TYPE=data_type if data_type == "raw" else "prob",
        batch=batch,
        hour=hour,
    )
    resp = requests.get(url)
    res_dict = {}
    if resp.status_code == 200:
        bs_items = BeautifulSoup(resp.text, "html.parser")
        files_list = [
            i["href"]
            for i in bs_items.find_all(
                "a", text=re.compile(f"CMC.*{date:%Y%m%d%H}.*.grib2")
            )
        ]
        for f in files_list:
            match = parse_filename(f, data_type=data_type)
            if match:
                v = V(match[1], match[2], match[3])
                o = var_dict.get(v)
                if o:
                    res_dict[f"{o.outname}-{match[6]}"] = {"url": os.path.join(url, f)}
    return res_dict


def download(**kwargs):
    single_session_download(
        download_url=kwargs["url"], local_fp=kwargs["local_fp"].replace(".nc", ".grib2")
    )
    if kwargs.get("stats") is None:
        single_ens_mean(
            kwargs["local_fp"].replace(".nc", ".grib2"),
            kwargs["local_fp"],
            os.path.basename(kwargs["local_fp"]).split("-")[0],
        )
    else:
        single_ens_stats(
            kwargs["local_fp"].replace(".nc", ".grib2"),
            kwargs["local_fp"],
            os.path.basename(kwargs["local_fp"]).split("-")[0],
            kwargs.get("stats"),
        )
    os.remove(kwargs["local_fp"].replace(".nc", ".grib2"))
    return os.path.getsize(kwargs["local_fp"])


def save_geps_ens(date: datetime, local_dir: str, product="raw"):
    """save all geps_ens batch files at date in directory

    Parameters:
        date: datetime, UTC
        local_dir: str, save dir
        product: str, geps_ens product, 'raw'/'products'
    return:
        -1 if some fail
        0 if all success
    """
    batch = date.hour
    logger.info(f"GEPS_ENS {date:%Y%m%d}{str(batch).zfill(2)} download start")
    hours = (
        HOURS["subseason"] if date.weekday() in [3] and batch == 0 else HOURS["medium"]
    )
    results = []
    fail = []
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as pool:
        for hour in hours[:]:
            urls = get_files_dict(date, hour, data_type=product)
            url_fp_list = [
                (v["url"], os.path.join(local_dir, os.path.basename(v["url"])))
                for k, v in urls.items()
            ]
            # url_fp_list = [(v,os.path.join(local_dir,f'{k}.grib2')) for k,v in urls.items()]
            results.append(pool.submit(batch_session_download, url_fp_list=url_fp_list))
        for n, r in enumerate(as_completed(results)):
            res = r.result()
            if res:
                fail.extend(res)
            else:
                logger.info(
                    f"GEPS_ENS: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)} HOUR: {hours[n]}] DOWNLOAD FINISH"
                )

    fail = batch_session_download(fail)
    if fail:
        logger.error("the following download fail")
        logger.error(fail)
        return -1
    else:
        logger.info(
            f"GEPS_ENS: [DATE: {date:%Y%m%d} BATCH: {str(batch).zfill(2)}] ALL DOWNLOAD FINISH"
        )
        return 0


def cal_geps_ens_stats(
    grib_dir: str,
    out_dir: str,
    stats: str,
    varname_suffix: bool = False,
    split_rule: str = os.path.join(MAESTERS, "static/pf_split"),
):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, 0o777, exist_ok=True)
    files = glob(os.path.join(grib_dir, "*.grib*"))
    fns = [os.path.basename(f) for f in files]
    matches = [parse_filename(fn) for fn in fns]
    in_out_var_list = []
    for n, m in enumerate(matches):
        if m:
            v = V(m[1], m[2], m[3])
            o = CMC_GEPS_ENS.variable.get(v)
            if o:
                name = o.outname + stats.upper() if varname_suffix else o.outname
                t = (files[n], os.path.join(out_dir, f"{o.outname}-{m[6]}.nc"), name)
                in_out_var_list.append(t)
    fail = batch_ens_stats(in_out_var_list, stats, split_rule)

    fail = batch_ens_stats(fail, stats, split_rule)
    if fail:
        logger.error(f"the following cal {stats} fail")
        logger.error(fail)
        return -1
    else:
        logger.info(f"GEPS_ENS: ALL {stats.upper()} CALC FINISH")
        return 0


def cal_geps_ens_mean(
    grib_dir: str,
    out_dir: str,
    split_rule: str = os.path.join(MAESTERS, "static/pf_split"),
):
    return cal_geps_ens_stats(grib_dir, out_dir, "ensmean", split_rule)


@retry(stop_max_delay=3 * 60 * 60 * 10e3, stop_max_attempt_number=1)
def operation(local_dir: str = None, rm_origin: bool = True, **kwargs):
    now = datetime.utcnow() - timedelta(hours=6)
    batch = int(now.hour / 12) * 12
    tmp_dir = (
        now.strftime(
            os.path.join(
                CMC_GEPS_ENS.data_dir.replace("~", os.environ.get("HOME")) + "_tmp",
                f"%Y%m%d{str(batch).zfill(2)}0000",
            )
        )
        if local_dir is None
        else local_dir + "_tmp"
    )
    local_dir = (
        now.strftime(
            os.path.join(
                CMC_GEPS_ENS.data_dir.replace("~", os.environ.get("HOME")),
                f"%Y%m%d{str(batch).zfill(2)}0000",
            )
        )
        if local_dir is None
        else local_dir
    )

    save_geps_ens(now.replace(hour=batch), tmp_dir)
    if kwargs.get("stats") is None:
        cal_geps_ens_mean(tmp_dir, local_dir)
    elif isinstance(kwargs.get("stats"), str):
        cal_geps_ens_stats(
            tmp_dir, local_dir, stats=kwargs.get("stats"), varname_suffix=True
        )

    if rm_origin:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        operation()
    else:
        operation(sys.argv[1])
