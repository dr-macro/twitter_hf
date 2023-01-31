import os
from tracemalloc import start
from typing import Dict
import tweepy

from scraper import Scraper
import datetime
import pandas as pd
import time

BEARER_TOKEN = os.getenv('BEARER_TOKEN')
AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')

twitter_account = ['pmje73',
 'ShortsellerST',
 'prometheusmacro',
 'just_economics',
 'antonhowes',
 'MFratzscher',
 'hendry_hugh',
 'FedGuy12',
 'MauriceHoefgen',
 'chigrl',
 'LynAldenContact',
 'PauloMacro',
 'samikaski',
 'amaurihsouza',
 'SenateGOP',
 'SenateDems',
 'FLOTUS',
 'JoeBiden',
 'FrankLuntz',
 'alicemhancock',
 'matteosalvinimi',
 'karpathy',
 'stanfordnlp',
 'OfficeforAI',
 'benblume',
 'Alex_vee123',
 'PunchableFaceII',
 'CompoundExotic',
 'UpslopeCapital',
 'Deezee1031',
 'DoxasticCap',
 'fundiescapital',
 'BongCapital',
 'bauhiniacapital',
 'SkeleCap',
 'NeuralBricolage',
 'MaxwellFrostFL',
 'ewarren',
 'BarackObama',
 'BernieSanders',
 'SenSchumer',
 'AndrewYang',
 'CoryBooker',
 'RepMaxineWaters',
 'MikeBloomberg',
 'MarkWarner',
 'SenDuckworth',
 'SenFeinstein',
 'ossoff',
 'POTUS',
 'CarterLibrary',
 'PeteButtigieg',
 'AOC',
 'WhereisRussia',
 'agurevich23',
 'VincentDeluard',
 'sacca',
 'macrocredit',
 'JohnArnoldFndtn',
 'PriapusIQ',
 'InvestLikeBest',
 'mgertken',
 'BaronDavis',
 'SavaryMathieu',
 'JacobShap',
 'KamranBokhari',
 'FinancePhoton',
 'nu_phases',
 'RPEddy',
 'Citrini7',
 'TheMarketHuddle',
 'vinelli',
 'patrick_saner',
 'JeremyDSchwartz',
 'timjohnbyford',
 'TotemMacro',
 'austen_g91',
 'GrindingNumbers',
 'WallWorry',
 'thefatjewish_',
 'shouldhaveaduck',
 'LowAlphaHighVol',
 'BenniKim',
 'GlobalProTrader',
 '0xHamz',
 'wadhwa',
 'pineconemacro',
 'shortl2021',
 'TgMacro',
 'GreekFire23',
 'donnelly_brent',
 'dampedspring',
 'profplum99',
 'AndreasSteno',
 'odsc',
 'ojblanchard1',
 'KpsZSU',
 'UAWeapons',
 'Podolyak_M',
 'FundamentEdge',
 'CliffordAsness',
 'jam_croissant',
 'AgustinLebron3',
 'bennpeifert',
 'Geo_papic',
 'ilangur',
 'kanyewest',
 'sentimentrader',
 'RikeFranke',
 'sguriev',
 'thiloalbers',
 'Wouter_Den_Haan',
 'martin_wiesmann',
 'jsuedekum',
 'GrimmVeronika',
 'ben_moll',
 'CVolkmannMD',
 'michaeljburry',
 'SecYellen',
 'LHSummers',
 'ptrubowitz',
 'sebastian_shemi',
 'DAlperovitch',
 'KofmanMichael',
 'DavidDjaiz',
 'nntaleb',
 'raoult_didier',
 'OlafScholz',
 'R2Rsquared']



#time.sleep(60)

try:
    client = tweepy.Client(bearer_token=BEARER_TOKEN)
except:
    print("Client error, check your bearer token or tweepy documentation")

def create_time_range():
    current = datetime.datetime.now() - datetime.timedelta(days=1)
    start_date = datetime.datetime(
        year=current.year,
        month=current.month,
        day=current.day, 
        hour=current.hour,
        minute=0
    )
    end_date = start_date + datetime.timedelta(hours=24)
    return start_date, end_date

start_date, end_date = create_time_range()

A = Scraper(
    twitter_account=twitter_account,
    client=client
)

df2 = pd.read_csv("data/user_ID_by_country.csv")
countries_of_interest = ['United States', 'United Kingdom', 'European Parliament', 'France', 'Germany', 'Australia', 'Turkey']
tweets_by_country = {}
for country in countries_of_interest:
    accounts_of_interest = list(df2[df2['Country']==country]['user_id'].unique())
    
    print(country)
    data = A._get_tweets_from_ids(client, accounts_of_interest, start_date, end_date)
    
    df_data = A._transform_data(data)
    
    tweets_by_country[country] = df_data
    
    localtime = time.localtime()
    result = time.strftime("%I:%M:%S %p", localtime)
    print(result)
    
    if country != countries_of_interest[-1]:
        print('Wait 15 minutes before next wave of requests')
        time.sleep(900)
       

for k in tweets_by_country.keys():
    filename = "{}_{}_to_{}.parquet".format(k, start_date, end_date).replace(':', '-') #Having ":" in filenames creates problems for AWS
    
    df_country = tweets_by_country[k]
    df_country.to_parquet(filename)
    A.export_data(
        key_id=AWS_ACCESS_KEY_ID,
        access_key=AWS_SECRET_ACCESS_KEY,
        file_name=filename
    )
    
  

