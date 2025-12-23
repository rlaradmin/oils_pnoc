# libraries
import pandas as pd
import numpy as np
from datetime import date

# excel read
path = input('Introduce the path and File name: ')
file = path.strip('"')

def oil_submission(le_type):
    '''The below initial Function reads and extracts row data from each tab, clean it and 
    prepare it to work in the Summaries (next functions from the query). '''

    oil_type = {
         'soybean_2025' : {'NA SBO 2025': {'cols': 'c:d', 'skiprows': 24, 'col_num':[2,3]}},
        'soybean_2026': {'NA SBO 2026': {'cols': "c:d", 'skiprows': 24,'col_num':[2,3]}},
        'soybean_2027':{'NA SBO 2027': {'cols': "c:d", 'skiprows': 24,'col_num':[2,3]}},
        'palm_2026': {'NA Palm 2026': {'cols': "d:e", 'skiprows': 50,'col_num':[3,4]}},
        'palm_2025':{'NA Palm 2025': {'cols': "d:e", 'skiprows': 32,'col_num':[3,4]}}
        }
        
    for sheet_name,params in oil_type[le_type].items():
            usecols = params['cols']
            skiprows = params['skiprows']
            df = pd.read_excel(file,
                                    sheet_name=sheet_name,
                                    usecols=usecols,
                                    skiprows=skiprows,
                                    engine="openpyxl")
            # Rename columns Unnamed as result of special selection from skiprows
            df.rename(columns={f'Unnamed: {params["col_num"][0]}':'lots',
                                 f'Unnamed: {params["col_num"][1]}':'rate'},inplace=True)
            
            # Apply .strip() to all DataFrame fields and remove rows where all fields are blank
            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df = df[~(df == '').all(axis=1)]

            # Correction of all the concepts eliminating typos and standarize strings.
            corrections = {
                'structured products': 'accumulators',
                'sturctured products': 'accumulators',
                'options': 'options',
                'futures / swaps': 'futures',
                'futures / swap s': 'futures',
                'forward contracting':'futures',
                'physical contracting': 'futures',
                'swaps':'futures',
                'feb':'february','mar':'march','sep':'september',
                'oct':'october', 'nov':'november', 'dec':'december'
                }
            df['lots'] = (df['lots']
                                    .astype(str)
                                    .str.lower()
                                    .str.strip()
                                    .replace(corrections))
            
            # Filters the data set to all different words in the list given after the .isin.
            df = df[~df['lots'].isin(['"sb" futures', '# of contracts','CPO (MT)'])]
            
            # Copy the words in the two lists into a new column, then this is used to fulfill all the lines below this word in the new column.
            # Words already in the column to be used to complete in the new column. 

            blok1 = ['january','february','march','april','may','june',
                    'july','august','september','october','november','december','q1','q2','q3','q4']
            blok2 = ['structured products', 'options','futures swaps',
                    'physical contracting','swaps','forward contracting','futures']

            # forward fill month
            df['month'] = df['lots'].where(df['lots'].isin(blok1))
            df['month'] = df['month'].ffill()

            # forward fill product
            df['product'] = df['lots'].where(df['lots'].isin(blok2))
            df['product'] = df['product'].ffill()

            # Reorganize columns: move 'month' and 'product' to the beginning
            columns_to_move = ['month', 'product']
            new_order = columns_to_move + [col for col in df.columns if col not in columns_to_move]
            df = df[new_order]

            # Converts to numeric value all the cells per column then if an error happends we take advantage of that making all error to nothing. 
            df = df[pd.to_numeric(df['rate'], errors='coerce').notnull()]
            df = df[pd.to_numeric(df['lots'], errors='coerce').notnull()]

            # Eliminates all the lines where there are no values or it is 'NaN'
            df.dropna(subset=['rate'],inplace = True)
            df.dropna(subset=['lots'],inplace = True)

            # Converts the columns to float type.
            df['lots'] = (df['lots']).astype(float)
            df['rate'] = (df['rate']).astype(float)

            # Drops all the lines with 0 in column lots.
            df = df[~df['lots'].isin([0,"",np.nan])]

            # Inclusion of a additional column with the spend(lots * rate)
            df['spend'] = df['lots'] * df['rate']
             
    return df

# Summarizing 'soybean' oils scenarios by lots and rates in quarters. 

def soybean_scenario (scenario_soybean):
    ''' This Function summarizes the detail of Soybean data 
    into a simple chart by quarter with volume and rate.'''

    df_soybean = oil_submission(scenario_soybean)
    month_to_qtr = {
        'january':'q1', 'february':'q1', 'march':'q1',
        'april':'q2', 'may':'q2', 'june':'q2', 'july':'q3',
        'august':'q3', 'september':'q3', 'october':'q4', 
        'november':'q4', 'december':'q4'
    }

    df_soybean['qtr'] = df_soybean['month'].map(month_to_qtr)
    col_qrt = df_soybean.pop('qtr')
    df_soybean.insert(0,'qtr',col_qrt)

    df_soybean_rate = df_soybean.groupby(['qtr','product']).agg(
        lots = ('lots','sum'),
        spend = ('spend','sum')
    )

    df_soybean_rate['rate'] = df_soybean_rate['spend']/df_soybean_rate['lots']
    df_soybean_rate = df_soybean_rate['rate'].unstack('qtr').reset_index()

    df_soybean_lots = df_soybean.groupby(['qtr','product'])['lots'].sum()
    df_soybean_lots = df_soybean_lots.unstack('qtr').reset_index()
    df_test_concat = pd.concat([df_soybean_lots,df_soybean_rate],keys=['lots','rate'])

    return df_test_concat

# sumirize 'Palm' oils scenarios by lots and rates.

def palm_scenario(scenario_palm):
    ''' This Function summarizes the detail of Soybean data 
    into a simple chart by quarter with volume and rate.'''
        
    df_palm = oil_submission(scenario_palm)
    df_palm_rate = df_palm.groupby(['month','product']).agg(
        lots = ('lots','sum'),
        spend = ('spend','sum')
    )
    df_palm_rate['rate'] = df_palm_rate['spend']/df_palm_rate['lots']
    df_palm_rate = df_palm_rate['rate'].unstack('month').reset_index()

    df_palm_lots = df_palm.groupby(['month','product'])['lots'].sum()
    df_palm_lots = df_palm_lots.unstack('month').reset_index()
    df_palm_concat = pd.concat([df_palm_lots,df_palm_rate],keys=['lots','rate'])

    return df_palm_concat

# Variables by scenario to make the Functions Work.

palm_26 = palm_scenario('palm_2026')
palm_25 = palm_scenario('palm_2025')
soybean_27 = soybean_scenario('soybean_2027')
soybean_26 = soybean_scenario('soybean_2026')
soybean_25 = soybean_scenario('soybean_2025')
# Creation of Excel File by Scenario.

path = input('Where to save the Final File? Paste the path: ')
final_name =f'{path}/Oils Summary {date.today()}.xlsx'.strip('"')

with pd.ExcelWriter(final_name) as writer:
    palm_25.to_excel(writer,sheet_name='Palm 2025')
    palm_26.to_excel(writer,sheet_name='Palm 2026')
    soybean_25.to_excel(writer,sheet_name='Soybean 2025')
    soybean_26.to_excel(writer,sheet_name='Soybean 2026')
    soybean_27.to_excel(writer,sheet_name='Soybean 2027')

print('✅​ Summary file created Successfully')
