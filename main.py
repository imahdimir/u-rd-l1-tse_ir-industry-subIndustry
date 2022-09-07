"""

  """

import json
from datetime import date

import pandas as pd
from githubdata import GithubData
from mirutil import utils as mu
from mirutil.df_utils import read_data_according_to_type as read_data
from mirutil.df_utils import save_as_prq_wo_index as sprq


class GDUrl :
    src = 'https://github.com/imahdimir/rd-tse_ir-industry-subIndustry'
    trg = 'https://github.com/imahdimir/d-tse_ir-industry-subIndustry'
    cur = 'https://github.com/imahdimir/u-d-tse_ir-industry-subIndustry'

gdu = GDUrl()

class ColName :
    inds = 'Industry'
    sub_inds = 'SubIndustry'
    cname = 'CompanyName'
    tic = 'Ticker'
    obsd = 'ObsDate'

c = ColName()

def main() :
    pass

    ##

    gd_src = GithubData(gdu.src)
    gd_src.overwriting_clone()
    ##
    fp = gd_src.local_path / 'data.json'
    with open(fp , 'r') as f :
        data = json.load(f)
    ##
    dc = data['data']
    ##
    df = pd.DataFrame(dc)

    df = df.rename(
            columns = {
                    'i' : c.inds
                    }
            )
    ##
    df = df.explode('z')
    ##
    df[c.sub_inds] = df['z'].apply(lambda x : x['t'])
    ##
    df['z'] = df['z'].apply(lambda x : x['z'])
    ##
    df = df[[c.inds , c.sub_inds , 'z']]
    ##
    df = df.explode('z')
    ##
    key_cn = {
            't'  : c.cname ,
            'sy' : c.tic ,
            'ic' : 'ic' ,
            'c'  : 'c' ,
            's'  : 's'
            }

    for ky , val in key_cn.items() :
        df[val] = df['z'].apply(lambda x : x[ky] if pd.notna(x) else None)

    ##
    df.drop(columns = ['z'] , inplace = True)
    ##
    df = df.drop_duplicates()
    ##
    df[c.obsd] = date.today().strftime('%Y-%m-%d')
    ##

    gd_trg = GithubData(gdu.trg)
    gd_trg.overwriting_clone()
    ##
    fp = gd_trg.data_fp
    ##
    dtr = gd_trg.read_data()
    ##
    df1 = pd.concat([dtr , df] , ignore_index = True)
    ##
    df1 = df1.sort_values('Date')
    ##
    df1 = df1.drop_duplicates(subset = df1.columns.difference(['Date']))
    ##
    sprq(df1 , fp)
    ##
    tokp = '/Users/mahdi/Dropbox/tok.txt'
    tok = mu.get_tok_if_accessible(tokp)
    ##
    msg = 'builded by: '
    msg += gdu.cur
    ##
    gd_trg.commit_and_push(msg , user = gd_trg.user_name , token = tok)
    ##


    gd_trg.rmdir()
    gd_src.rmdir()

    ##

##
if __name__ == '__main__' :
    main()

    ##

    ##
