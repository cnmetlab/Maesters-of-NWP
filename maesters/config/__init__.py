from dataclasses import dataclass

@dataclass
class V:
    varname: str
    level_type: str
    level: str

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.varname == other.varname) & (self.level_type == other.level_type) & (self.level == self.level)
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

ECCC_GEPS_ENS_VARIABLES= {
    V('SWAT',      'DBLL',   '10cm'):  O('SWAT_CM10'),
    V('TSOIL',     'DBLL',   '10cm'):  O('TSOIL_CM10'),
    V('RH',        'TGL',    '2m'):    O('RHU_L0'),
    V('TMP',       'TGL',    '2m'):    O('TMP_L0'),
    V('TMP',       'TGL',    '40'):    O('TMP_M40'),
    V('TMP',       'TGL',    '80'):    O('TMP_M80'),
    V('TMP',       'TGL',    '120'):   O('TMP_M120'),
    V('UGRD',      'TGL',    '10m'):   O('U_M10'),
    V('VGRD',      'TGL',    '10m'):   O('V_M10'),
    V('SPFH',      'TGL',    '2'):     O('SPFH_L0'),
    V('SPFH',      'TGL',    '40'):    O('SPFH_M40'),
    V('SPFH',      'TGL',    '80'):    O('SPFH_M80'),
    V('SPFH',      'TGL',    '120'):   O('SPFH_M120'),
    V('WIND',      'TGL',    '10'):    O('WIND_M10'),
    V('WIND',      'TGL',    '40'):    O('WIND_M40'),
    V('WIND',      'TGL',    '80'):    O('WIND_M80'),
    V('WIND',      'TGL',    '120'):   O('WIND_M120'),
    V('APCP',      'SFC',    '0'):     O('TP_L0'),
    V('AFRAIN',    'SFC',    '0'):     O('AFRAIN_L0'),
    V('AICEP',     'SFC',    '0'):     O('AICEP_L0'),
    V('ARAIN',     'SFC',    '0'):     O('AFRAIN_L0'),
    V('ASNOW',     'SFC',    '0'):     O('ASNOW_L0'),
    V('CAPE',      'SFC',    '0'):     O('CAPE_L0'),
    V('CIN',       'SFC',    '0'):     O('CIN_L0'),
    V('DSWRF',     'SFC',    '0'):     O('DSWRF_L0'),
    V('DLWRF',     'SFC',    '0'):     O('DLWRF_L0'),
    V('ICETK',     'SFC',    '0'):     O('ICETK_L0'),
    V('LHTFL',     'SFC',    '0'):     O('LHTFL_L0'),
    V('OLR',       'NTAT',   '0'):     O('OLR_L0'),
    V('PRMSL',     'MSL',    '0'):     O('PRMSL_S0'),
    V('PWAT',      'EATM',   '0'):     O('PWAT_L0'),
    V('PRES',      'SFC',    '0'):     O('PRES_L0'),
    V('SHTFL',     'SFC',    '0'):     O('SHTFL_L0'),
    V('SNOD',      'SFC',    '0'):     O('SNOD_L0'),
    V('SFCWRO',    'SFC',    '0'):     O('SFCWRO_L0'),
    V('TCDC',      'SFC',    '0'):     O('TCC_L0'),
    V('ULWRF',     'SFC',    '0'):     O('ULWRF_L0'),
    V('USWRF',     'SFC',    '0'):     O('USWRF_L0'),
    V('WEASD',     'SFC',    '0'):     O('WEASD_L0'),
    # V('HGT',       'ISBL',   '0010'):  O('HGT_P010'),
    # V('RH',        'ISBL',   '0010'):  O('RHU_P010'),
    # V('TMP',       'ISBL',   '0010'):  O('TMP_P010'),
    # V('UGRD',      'ISBL',   '0010'):  O('U_P010'),
    # V('VGRD',      'ISBL',   '0010'):  O('V_P010'),
    # V('HGT',       'ISBL',   '0050'):  O('HGT_P050'),
    # V('RH',        'ISBL',   '0050'):  O('RHU_P050'),
    # V('TMP',       'ISBL',   '0050'):  O('TMP_P050'),
    # V('UGRD',      'ISBL',   '0050'):  O('U_P050'),
    # V('VGRD',      'ISBL',   '0050'):  O('V_P050'),
    # V('HGT',       'ISBL',   '0100'):  O('HGT_P100'),
    # V('RH',        'ISBL',   '0100'):  O('RHU_P100'),
    # V('TMP',       'ISBL',   '0100'):  O('TMP_P100'),
    # V('UGRD',      'ISBL',   '0100'):  O('U_P100'),
    # V('VGRD',      'ISBL',   '0100'):  O('V_P100'),
    # V('HGT',       'ISBL',   '0200'):  O('HGT_P200'),
    # V('RH',        'ISBL',   '0200'):  O('RHU_P200'),
    # V('TMP',       'ISBL',   '0200'):  O('TMP_P200'),
    # V('UGRD',      'ISBL',   '0200'):  O('U_P200'),
    # V('VGRD',      'ISBL',   '0200'):  O('V_P200'),
    # V('HGT',       'ISBL',   '0250'):  O('HGT_P250'),
    # V('RH',        'ISBL',   '0250'):  O('RHU_P250'),
    # V('TMP',       'ISBL',   '0250'):  O('TMP_P250'),
    # V('UGRD',      'ISBL',   '0250'):  O('U_P250'),
    # V('VGRD',      'ISBL',   '0250'):  O('V_P250'),
    # V('HGT',       'ISBL',   '0300'):  O('HGT_P300'),
    # V('UGRD',      'ISBL',   '0300'):  O('U_P300'),
    # V('VGRD',      'ISBL',   '0300'):  O('V_P300'),
    # V('UGRD',      'ISBL',   '0400'):  O('U_P400'),
    # V('VGRD',      'ISBL',   '0400'):  O('V_P400'),
    V('HGT',       'ISBL',   '0500'):  O('HGT_P500'),
    V('RH',        'ISBL',   '0500'):  O('RHU_P500'),
    V('TMP',       'ISBL',   '0500'):  O('TMP_P500'),
    V('UGRD',      'ISBL',   '0500'):  O('U_P500'),
    V('VGRD',      'ISBL',   '0500'):  O('V_P500'),
    V('HGT',       'ISBL',   '0700'):  O('HGT_P700'),
    V('RH',        'ISBL',   '0700'):  O('RHU_P700'),
    V('TMP',       'ISBL',   '0700'):  O('TMP_P700'),
    V('UGRD',      'ISBL',   '0700'):  O('U_P700'),
    V('VGRD',      'ISBL',   '0700'):  O('V_P700'),
    V('HGT',       'ISBL',   '0850'):  O('HGT_P850'),
    V('VVEL',      'ISBL',   '0850'):  O('VVEL_P850'),
    V('RH',        'ISBL',   '0850'):  O('RHU_P850'),
    V('TMP',       'ISBL',   '0850'):  O('TMP_P850'),
    V('UGRD',      'ISBL',   '0850'):  O('U_P850'),
    V('VGRD',      'ISBL',   '0850'):  O('V_P850'),
    V('HGT',       'ISBL',   '0925'):  O('HGT_P925'),
    V('RH',        'ISBL',   '0925'):  O('RHU_P925'),
    V('TMP',       'ISBL',   '0925'):  O('TMP_P925'),
    V('UGRD',      'ISBL',   '0925'):  O('U_P925'),
    V('VGRD',      'ISBL',   '0925'):  O('V_P925'),
    V('HGT',       'ISBL',   '1000'):  O('HGT_P1000'),
    V('RH',        'ISBL',   '1000'):  O('RHU_P1000'),
    V('TMP',       'ISBL',   '1000'):  O('TMP_P1000'),
    V('UGRD',      'ISBL',   '1000'):  O('U_P1000'),
    V('VGRD',      'ISBL',   '1000'):  O('V_P1000'),
}

ECCC_GEPS_ENS = MODEL(
    'eccc_geps_ens',ECCC_GEPS_ENS_VARIABLES,'/var/lib/data/eccc/GEPS_ENS/orig','/var/lib/data/eccc/GEPS_ENS/archive','https://dd.weather.gc.ca/ensemble/geps/grib2/{PRODUCT}/{batch}/{hour}/'
    )
    
PATH = '/home/zhan.li/miniconda3/envs/maesters/bin'

if __name__ == "__main__":
    origin = set([f'{i.varname}_{i.level_type}_{i.level}' for i in list(ECCC_GEPS_ENS_VARIABLES.keys())])
    print(ECCC_GEPS_ENS_VARIABLES.get(V('VGRD',      'ISBL',   '1000')))
    print(origin)