from datetime import datetime,timedelta
import os,sys
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(__file__))
from config import MODELS,DEFAULT_MAESTER

import xarray as xr

MAX_NUMBER = 5

class Maester:
    def __init__(
        self,
        source:str =DEFAULT_MAESTER.get('source'), 
        product:str = DEFAULT_MAESTER.get('product'),
        varname:str = DEFAULT_MAESTER.get('varname'),
        batch: datetime = None, # nwp batch start predict time UTC
        date: datetime = None, # nwp predict variable time UTC
        hour: int = None, # nwp predict variable hour from start predict time
        datahome:str = DEFAULT_MAESTER.get('datahome'),
        **kwargs,
        # data_type: data type for ENS prediction to ECMWF ENFO, 'cf'/'pf1'/'pf2'/.../
        # stats: stats method for ENS prediction enfo or geps 'ensmean'/'ensmax'/'ensmin'
        ) -> None:

        self.source = source
        self.product = product
        self.datahome = datahome
        self.varname = varname
        self.model = MODELS.get(f'{self.source}_{self.product}')

        if isinstance(date,str):
            date = datetime.strptime(date,'%Y-%m-%d %H:%M')
        
        if hour is None and date:
            now = datetime.utcnow().replace(minute=0,second=0,microsecond=0)
            self.batch = now.replace(hour=int((now-timedelta(hours=self.model.delay_hours)).hour/12)*12)
            if isinstance(date,datetime):
                self.hour = int((date - self.batch).total_seconds()/3600)
            elif isinstance(date,list):
                self.hour = []
                for d in date:
                    if isinstance(d,str):d=datetime.strptime(d,'%Y-%m-%d %H:%M')
                    self.hour.append(int((d - self.batch).total_seconds()/3600))

        if isinstance(batch,datetime):
            self.batch = batch
        elif isinstance(batch,str):
            self.batch = datetime.strptime(batch,'%Y-%m-%d %H:%M')

        for k,v in kwargs.items():
            setattr(self,k,v)


        # import model get file dict method
        exec(f"from citadels.{self.source.upper()}.{self.product.lower()} import get_files_dict as get_{source}_{product}_files_dict ;self._get_files_dict = get_{source}_{product}_files_dict")
        for k,v in self.model.variable.items():
            if v.outname == varname:
                self.variable = k
                self.out = v
                break

        if isinstance(self.hour,int):
            self.download_dict = self._get_files_dict(date=self.batch,hour=self.hour,var_dict={self.variable:self.out},**kwargs)
        elif isinstance(hour,list):
            self.download_dict ={}
            for h in self.hour:
                d = self._get_files_dict(date=self.batch,hour=h,var_dict={self.variable:self.out},**kwargs)
                for k,v in d.items():
                    self.download_dict[k] = v
    

    def download(self,local_dir:str=None):
        # import model download method
        exec(f"from citadels.{self.source.upper()}.{self.product.lower()} import download;self._download = download")
        self.local_fp = []
        res = []
        with ThreadPoolExecutor(max_workers=MAX_NUMBER) as pool:
            for k,v in self.download_dict.items():
                local_fp = os.path.join(local_dir,f'{k}.nc') if local_dir else \
                    os.path.join(self.datahome,f'{self.source}',f'{self.product}',self.batch.strftime('%Y%m%d%H0000'),f'{k}.nc')
                v['local_fp'] = local_fp
                res.append(pool.submit(self._download,**v))
                self.local_fp.append(local_fp)
            for r in as_completed(res):
                r.result()
    
    def operation(self,local_dir:str=None):
        # import model operation method
        exec(f"from citadels.{self.source.upper()}.{self.product.lower()} import operation;self._operation = operation")
        self.operation(local_dir)

    
    def xarray(self):
        if hasattr(self,'local_fp'):
            return xr.open_mfdataset(self.local_fp,combine='nested')
        else:
            self.download()
            return xr.open_mfdataset(self.local_fp,combine='nested',concat_dim='time')


if __name__ == '__main__':
    # m  = Maester(source='ecmwf',product='enfo')
    # r = m.get_files_dict(datetime(2022,6,25,0,0),3,'TMP_L0')

    # m  = Maester(source='dwd',product='icon')
    # r = m.get_files_dict(datetime(2022,6,25,0,0),3,'TMP_L0')

    # cmc  = Maester(source='cmc',product='geps_ens',date=datetime(2022,6,25,12,0),hour=3,varname='TMP_L0')
    # cmc_data = cmc.xarray()
    # ecmwf  = Maester(source='ecmwf',product='enfo',date=datetime(2022,6,25,12,0),hour=3,varname='TMP_L0')
    # ecmwf_data = ecmwf.xarray()
    # dwd = Maester(source='dwd',product='icon',date=datetime(2022,6,25,12,0),hour=3,varname='TMP_L0')
    # dwd_data = dwd.xarray()
    ecmwf  = Maester(source='ecmwf',product='enfo',date=datetime(2022,6,25,12,0),hour=[6,9,12],varname='TMP_L0')
    ecmwf_data = ecmwf.xarray()
    print()



        
        
        

        
    


