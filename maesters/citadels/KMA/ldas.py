import os
import sys
from datetime import datetime, timedelta

from loguru import logger
import pandas as pd

from maesters.config import NCEP_FNL, V, PATH
from maesters.utils import auth_download

MAESTERS = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

passwd = None
if len(sys.argv) < 3:
    print("usage: " + sys.argv[0] + " [-q] email password_on_RDA_webserver")
    if passwd is None:
        sys.exit(1)
else:
    passwd = sys.argv[2]
    email = sys.argv[1]

if len(sys.argv) == 5:
    start_date = datetime.strptime(sys.argv[3], "%Y%m%d%H")
    end_date = datetime.strptime(sys.argv[4], "%Y%m%d%H")
else:
    start_date = (datetime.utcnow() - timedelta(days=3)).replace(hour=0)
    end_date = start_date.replace(hour=18)

auth = {
    "email": email,
    "password": passwd,
    "action": "login",
}

logger.info(auth)


def get_urls(start_date: datetime, end_date: datetime) -> list:
    """get fnl download urls

    Parameters:
        start_date: datetime, start date UTC
        end_date: datetime, end_date UTC
    Return:
        list, download urls list
    """
    dates = pd.date_range(
        start_date.strftime("%Y-%m-%d %H:00"),
        end_date.strftime("%Y-%m-%d %H:00"),
        freq="6H",
    )
    urls = [
        d.strftime(NCEP_FNL.download_url)
        if d > datetime(2007, 12, 6, 6)
        else d.strftime(NCEP_FNL.download_url)
        .replace("/OS/", "/")
        .replace("grib2", "grib1")
        .replace("http", "https")
        for d in dates
    ]
    return urls


client = auth_download()
client.login(NCEP_FNL.login_url, auth)

urls = get_urls(start_date, end_date)
url_fp_list = [(u, os.path.join(NCEP_FNL.data_dir, os.path.basename(u))) for u in urls]
logger.debug(url_fp_list)
client.batch_download(url_fp_list)
