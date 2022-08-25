from glob import glob
import shutil
from datetime import datetime, timedelta
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import warnings

# from subprocess import call

import requests
import boto3
import botocore
from loguru import logger
import pandas as pd
import numpy as np
from retrying import retry
from pandas.core.common import SettingWithCopyWarning

from maesters.config import get_model, V  # , PATH
from maesters.utils import batch_range_download, single_range_download
from maesters.utils.post_process import batch_convert_nc, single_convert_nc

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
MAESTERS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger.add(
    os.path.join(os.path.dirname(MAESTERS), "log/NOAA_GFS_{time:%Y%m%d}"),
    rotation="00:00",
    retention=10,
)


PARALLEL_NUM = 5

NOAA_GFS = get_model("noaa", "gfs")
HOURS = {
    "medium": list(range(0, 144, 3)) + list(range(144, 384 + 3, 3)),
}


def get_url_detail(s3url: str) -> pd.DataFrame:
    """get the detail messages of the grib2 file of the given url

    Args:
        url (str): grib file url

    Returns:
        pd.DataFrame: mesaages details
    """
    query_url = s3url.replace(".grib2", ".idx")

    res_dict = []
    try:
        client = boto3.client(
            "s3", config=botocore.client.Config(signature_version=botocore.UNSIGNED)
        )
        bucket = s3url.split("/")[0]
        prefix = "/".join(s3url.split("/")[1:])
        r = client.get_object(Bucket=bucket, Key=prefix)
        body = r.get("Body")
        var = [
            (i.split(":")[3], i.split(":")[1], i.split(":")[4])
            for i in body.decode("utf-8").split("\n")
            if len(i)
        ]
        df = pd.DataFrame(var, columns=["var", "bytes_start", "level"])
        r = requests.get(query_url)
        content = r.text
        for c in content.split("\n")[:]:
            if len(c) == 0:
                continue
            d = {}
            for k, v in json.loads(c).items():
                if k == "_offset":
                    d["start"] = str(v)
                if k == "_length":
                    d["end"] = str(int(d["start"]) + int(v))
                if k == "levelist":
                    if v is not None:
                        d[k] = int(v)
                    else:
                        d[k] = np.nan
                else:
                    d[str(k)] = str(v)
            if d.get("levelist") is None:
                if d["param"] == "2t":
                    d["levelist"] = 2
                elif d["param"] in ["10u", "10v"]:
                    d["levelist"] = 10
                else:
                    d["levelist"] = 0
            res_dict.append(d)
    except Exception as e:
        logger.error(e)
    df = pd.DataFrame(res_dict[:])
    return df


def get_files_dict(date: datetime, hour: int, var_dict=NOAA_GFS.variable) -> dict:
    """get download list at date batch hour

    Args:
        date (datetime): initial date
        hour (int): forecast hour
        var_dict (dict): variable dict
    Returns:
        dict: {
            '{VARNAME}_{LEVEL}-{HOUR}': {'url': str, 'start': str, 'end': str},
        }
    """
    url = date.strftime(NOAA_GFS.download_url).format(hour=int(hour))
    df = get_url_detail(url)

    # filter only cf
    try:
        df.loc[:, "fn"] = df.apply(
            lambda x: var_dict.get(
                V(x["param"], x["levtype"], str(x["levelist"]))
            ).outname
            if var_dict.get(V(x["param"], x["levtype"], str(x["levelist"])))
            else np.nan,
            axis=1,
        )
        res = df.dropna(axis=0, subset=["fn"])
        res.loc[:, "fn"] = res["fn"].apply(lambda x: f"{x}-{str(hour).zfill(3)}")
        res.loc[:, "url"] = url
        return {
            i[0]: {"url": i[1], "start": i[2], "end": i[3]}
            for i in res[["fn", "url", "start", "end"]].values
        }
    except Exception as e:
        logger.error(e)
        return {}


def get_all_files_list(dt: datetime) -> dict:
    result = []
    with ThreadPoolExecutor(max_workers=5) as exec:
        futures = [
            exec.submit(get_files_dict, date=dt, hour=h) for h in HOURS["medium"]
        ]
        for n, f in enumerate(as_completed(futures)):
            result.append(f.result())
    return result


def download(**kwargs):
    single_range_download(
        download_url=kwargs["url"],
        start_bytes=kwargs["start"],
        end_bytes=kwargs["end"],
        local_fp=kwargs["local_fp"].replace(".nc", ".grib2"),
    )
    single_convert_nc(kwargs["local_fp"].replace(".nc", ".grib2"), kwargs["local_fp"])
    os.remove(kwargs["local_fp"].replace(".nc", ".grib2"))
    return os.path.getsize(kwargs["local_fp"])


def save_ecmwf_oper(date: datetime, local_dir: str):
    """save all oper batch files at date in directory

    Parameters:
        date: datetime, UTC
        local_dir: str, save dir
    return:
        -1 if some fail
        0 if all success
    """
    logger.info(f"ECMWF_OPER {date:%Y%m%d}{str(date.hour).zfill(2)} download start")
    results = []
    fail = []
    downloads = get_all_files_list(date)
    with ProcessPoolExecutor(max_workers=PARALLEL_NUM) as exec:
        for n, d in enumerate(downloads):
            inputs_list = [
                (v["url"], v["start"], v["end"], os.path.join(local_dir, k + ".grib2"))
                for k, v in d.items()
            ]
            results.append(exec.submit(batch_range_download, inputs_list=inputs_list))

        for n, r in enumerate(as_completed(results)):
            res = r.result()
            if res:
                fail.extend(res)
            else:
                logger.info(
                    f'ECMWF_OPER: [DATE: {date:%Y%m%d} BATCH: {str(date.hour).zfill(2)} HOUR: {HOURS["medium"][n]}] DOWNLOAD FINISH'
                )

    fail = batch_range_download(fail, file_type="grib")
    if fail:
        logger.error("the following download fail")
        logger.error(fail)
        return -1
    else:
        logger.info(
            f"ECMWF_OPER: [DATE: {date:%Y%m%d} BATCH: {str(date.hour).zfill(2)}] ALL DOWNLOAD FINISH"
        )
        return 0


def convert_ecmwf_oper(grib_dir: str, out_dir: str):
    grib_files = glob(os.path.join(grib_dir, "*.grib*"))
    in_out_list = [
        (f, os.path.join(out_dir, os.path.basename(f).split(".gr")[0] + ".nc"))
        for f in grib_files
    ]
    fail = batch_convert_nc(in_out_list)
    fail = batch_convert_nc(fail)
    if fail:
        logger.error("the following convern nc fail")
        logger.error(fail)
        return -1
    else:
        logger.info(f"ECMWF_OPER: ALL CONVERT FINISH")
        return 0


@retry(stop_max_delay=3 * 60 * 60 * 10e3, stop_max_attempt_number=1)
def operation(local_dir: str = None):
    now = datetime.utcnow() - timedelta(hours=9)
    batch = int(now.hour / 12) * 12
    tmp_dir = (
        now.strftime(
            os.path.join(
                ECMWF_OPER.data_dir.replace("~", os.environ.get("HOME")) + "_tmp",
                f"%Y%m%d{str(batch).zfill(2)}0000",
            )
        )
        if local_dir is None
        else local_dir + "_tmp"
    )
    local_dir = (
        now.strftime(
            os.path.join(
                ECMWF_OPER.data_dir.replace("~", os.environ.get("HOME")),
                f"%Y%m%d{str(batch).zfill(2)}0000",
            )
        )
        if local_dir is None
        else local_dir
    )
    save_ecmwf_oper(now.replace(hour=batch), tmp_dir)
    convert_ecmwf_oper(tmp_dir, local_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        operation()
    else:
        operation(sys.argv[1])
