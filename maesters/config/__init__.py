from dataclasses import dataclass
from statistics import mode
import sys
import os
from subprocess import check_output
import toml

@dataclass
class V:
    varname: str
    level_type: str
    level: str

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.varname == other.varname) & (self.level_type == other.level_type) & (self.level == other.level)
        else:
            return False

    def __hash__(self):
        return hash(self.varname+self.level_type+self.level)

@dataclass
class O:
    outname: str

@dataclass
class MODEL:
    modelname: str
    variable: dict
    data_dir: str
    archive_dir: str
    download_url: str
    login_url: str

def get_model(name:str):
    with open(os.path.join(os.path.dirname(__file__),'models.toml'),'r') as f:
        models = toml.load(f)
    model = models.get(name)
    if model:
        varibales = model.get('variables')
        var_dict = {}
        if varibales is None:
            pass
        else:
            for v in varibales:
                var_dict[V(v[0],v[1],v[2])] = O(v[3])
        m = MODEL(name,var_dict,model.get('data_dir'),model.get('archive_dir'),model.get('download_url'),model.get('login_url'))
        return m
    else:
        return None



CMC_GEPS_ENS = get_model('cmc_geps_ens')
CMC_GEM = get_model('cmc_gem')
DWD_ICON = get_model('dwd_icon')
ECMWF_ENFO = get_model('ecmwf_enfo')
ECMWF_OPER = get_model('ecmwf_oper')

MODEL = {
    'cmc_gem': CMC_GEM,
    'cmc_geps_ens': CMC_GEPS_ENS,
    'dwd_icon': DWD_ICON,
    'ecmwf_enfo': ECMWF_ENFO,
    'ecmwf_oper': ECMWF_OPER,
}

DEFAULT_MAESTER = {
    'datahome': os.path.join(os.environ.get('HOME'),'data'),
    'source': 'ecmwf',
    'product': 'enfo',
    'varname': 'TMP_L0',
    'hour': 3,
}


if __name__ == "__main__":
    DWD_ICON
    print()
