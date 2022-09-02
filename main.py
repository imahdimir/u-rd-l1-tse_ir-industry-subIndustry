"""

  """

##


import pandas as pd
import requests
from ast import literal_eval
import json
from githubdata import GithubData
from mirutil.df_utils import save_df_as_a_nice_xl as snxl
from mirutil import utils as mu

class RepAdd:
  src = 'imahdimir/raw-d-tse_ir-industry-subIndustry'
  targ = 'imahdimir/d-tse_ir-industry-subIndustry'

  cur_url = 'https://github.com/imahdimir/b-d-tse_ir-industry-subIndustry'

ra = RepAdd()

def main() :

  pass

  ##
  rp_src = GithubData(ra.src)
  rp_src.clone()

  ##
  fp = rp_src.local_path / 'data.json'
  ##
  with open(fp, 'r') as f:
    data = json.load(f)
  ##
  dc = data['data']
  ##
  df = pd.DataFrame(dc)
  df = df.rename(columns = {
      'i' : 'Industry'
      })
  ##
  df = df.explode('z')
  ##
  df['SubIndustry'] = df['z'].apply(lambda x : x['t'])
  ##
  df['z'] = df['z'].apply(lambda x : x['z'])
  ##
  df = df[['Industry', 'SubIndustry', 'z']]
  ##
  df = df.explode('z')
  ##
  key_cn = {
      't': 'CompanyName',
      'sy' : 'Ticker',
      'ic' : 'ic',
      'c' : 'c',
      's' : 's'
      }

  for ky, val in key_cn.items():
    df[val] = df['z'].apply(lambda x : x[ky] if pd.notna(x) else None)

  ##
  df.drop(columns = ['z'], inplace = True)
  ##

  rp_tar = GithubData(ra.targ)
  rp_tar.clone()
  ##
  fp = rp_tar.data_fp
  ##
  snxl(df, fp)
  ##
  tokp = '/Users/mahdi/Dropbox/tok.txt'
  tok = mu.get_tok_if_accessible(tokp)

  ##
  msg = 'builded by: '
  msg += ra.cur_url
  ##
  rp_tar.commit_and_push(msg, user = rp_tar.user_name, token = tok)

  ##


  rp_tar.rmdir()
  rp_src.rmdir()

  ##

##
if __name__ == '__main__' :
  main()

  ##

  ##