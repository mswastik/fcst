from arrow_odbc import read_arrow_batches_from_odbc
from datetime import datetime
from dateutil.relativedelta import relativedelta
import polars as pl
import os
from nicegui import run

today=datetime.today()
path = os.path.expanduser("~")+'/fcst'
#try:
#    os.mkdir(path)
#    past_months=-3
#except:
#    past_months=-12
ss="gda-globalsynapseanalytics-ws-prod.sql.azuresynapse.net"

def sqlpd(loc='',reg='',prod='',fn='',pm=6,nm=6):
    print('Starting Query!!!')
    query=f'''
    SELECT
        [SellingDivision] as [Selling Division],[COUNTRY_GROUP] 'Area',[StrykerGroupRegion] as [Stryker Group Region],[Region],[Country],p.[CatalogNumber],
        p.[Business_Sector] as [Business Sector],p.[Business_Unit] as [Business Unit],p.[Franchise],p.[Product_Line] as [Product Line],p.[IBP_Level_5] as [IBP Level 5],
        p.[IBP_Level_6] as [IBP Level 6],p.[IBP_Level_7] as [IBP Level 7],[SALES_DATE],p.[xx_uom_conversion] as UOM ,p.[PackContent] AS [Pack Content],
        SUM([L0_ASP_Final_Rev]) [`L0 ASP Final Rev], SUM([Act_Orders_Rev]) "`Act Orders Rev",
        SUM([Act_Orders_Rev_Val]) "Act Orders Rev Val", SUM(s.[L2_DF_Final_Rev]) as [L2 DF Final Rev],
        SUM(s."L1_DF_Final_Rev") as [L1 DF Final Rev], SUM(s."L0_DF_Final_Rev") as [L0 DF Final Rev],
        SUM(s.[L2_Stat_Final_Rev]) as [L2 Stat Final Rev], SUM(Fcst_DF_Final_Rev) as [`Fcst DF Final Rev], SUM(Fcst_Stat_Final_Rev) as [`Fcst Stat Final Rev],
        SUM(Fcst_Stat_Prelim_Rev) as [`Fcst Stat Prelim Rev], SUM(Fcst_DF_Final_Rev_Val) as [Fcst DF Final Rev Val]
        
    FROM [Envision].[Demantra_CLD_Fact_Sales] s

    JOIN [Envision].[DIM_Demantra_CLD_products] p
    ON s.item_skey = p.demantra_item_skey AND p.[Current] = 'True'
            
    JOIN [Envision].[DIM_Demantra_CLD_DemantraLocation] l
    ON s.Location_sKey = l.Location_skey

    JOIN [Envision].[Dim_DEMANTRA_CLD_MDP_Matrix] m
    ON s.MDP_Key = m.MDP_Key

    WHERE
        [SALES_DATE] BETWEEN DATEADD(month, {-pm}, GETDATE()) AND DATEADD(month, {nm}, GETDATE()) AND
        [{loc}] in ('{reg}') AND
        --p.Franchise IN ('{fn}')
        --p.Franchise IN ({(','.join('?'*len(fn)))})
        p.[{prod}] IN ('{fn}')
            
    GROUP BY
        [SellingDivision],[COUNTRY_GROUP],[StrykerGroupRegion],[Region],[Country],p.[Business_Sector],p.[Business_Unit],p.[Franchise],
        p.[IBP_Level_5],p.[IBP_Level_6],p.[IBP_Level_7],p.[Product_Line],[SALES_DATE],p.[CatalogNumber],p.[xx_uom_conversion],p.[PackContent]'''
    connection_string=f"Driver={{ODBC Driver 17 for SQL Server}};Server={ss};database=gda_glbsyndb;Encrypt=Yes;Authentication=ActiveDirectoryInteractive;"
    try:
        reader = read_arrow_batches_from_odbc(query=query,connection_string=connection_string)
        df1=pl.DataFrame()
        for batch in reader:
            df1= pl.concat([df1,pl.from_arrow(batch)])
        print('Done!!!')
        print(df1)
        df1=df1.with_columns(pl.col('SALES_DATE').cast(pl.Datetime).dt.cast_time_unit('us'))
        df1.write_csv(f'C:\\Users\\{os.getlogin()}\\Downloads\\temp.csv')
        try:
            df=pl.read_parquet(f'data/{fn}.parquet')
            df=df.filter(pl.col('SALES_DATE')<=datetime(today.year,today.month,1)-relativedelta(months=3))
            df=pl.concat([df,df1])
            df.write_parquet(f'data/{fn}.parquet')
        except:
            df1.write_parquet(f'data/{fn}.parquet')
        try:
            ph=pl.read_parquet(f'data/phierarchy.parquet')
            ph=pl.concat(ph,df1['Business Sector','Franchise','Business Unit','Product Line','IBP Level 5','IBP Level 6','IBP Level 7','CatalogNumber'].unique())
            ph.unique().write_parquet(f'data/phierarchy.parquet')
        except:
            df['Business Sector','Franchise','Business Unit','Product Line','IBP Level 5','IBP Level 6','IBP Level 7','CatalogNumber'].unique().write_parquet(f'data/phierarchy.parquet')
        try:
            lh=pl.read_parquet(f'data/lhierarchy.parquet')
            lh=pl.concat(lh,df1['Stryker Group Region','Area','Region','Country'].unique())
            lh.unique().write_parquet(f'data/lhierarchy.parquet')
        except:
            df['Stryker Group Region','Area','Region','Country'].unique().write_parquet(f'data/lhierarchy.parquet')
    except Exception as e:
        print(f"Arrow ODBC Error: {e}")
    return False
    
async def phierarchy():
    query=f'''
    SELECT DISTINCT
        p.[BusinessSector] as [Business Sector],p.[BusinessUnit] as [Business Unit],p.[Franchise],p.[ProductLine] as [Product Line],
        p.[IBPLevel5] as [IBP Level 5],p.[IBPLevel6] as [IBP Level 6],p.[IBPLevel7] as [IBP Level 7],p.[CatalogNumber]

    FROM [Envision].[DIM_Demantra_CLD_demantraproducts] p
 '''
    connection_string=f"Driver={{ODBC Driver 17 for SQL Server}};Server={ss};database=gda_glbsyndb;Encrypt=Yes;Authentication=ActiveDirectoryInteractive;"
    reader = read_arrow_batches_from_odbc(query=query,connection_string=connection_string)
    df1=pl.DataFrame()
    print('Querying!!!')
    for batch in reader:
        df1= pl.concat([df1,await run.io_bound(pl.from_arrow,batch)])
    print('Done!!!')
    df1=df1.unique()
    df1.write_parquet(f'data//phierarchy.parquet')

async def lhierarchy():
    query=f'''
    SELECT DISTINCT
        [SellingDivision] as [Selling Division],[COUNTRY_GROUP] 'Area',[StrykerGroupRegion] as [Stryker Group Region],[Region],[Country]
            
    FROM [Envision].[DIM_Demantra_CLD_DemantraLocation] l
 '''
    connection_string=f"Driver={{ODBC Driver 17 for SQL Server}};Server={ss};database=gda_glbsyndb;Encrypt=Yes;Authentication=ActiveDirectoryInteractive;"
    reader = read_arrow_batches_from_odbc(query=query,connection_string=connection_string)
    df1=pl.DataFrame()
    print('Querying!!!')
    for batch in reader:
        df1= pl.concat([df1,await run.io_bound(pl.from_arrow,batch)])
    print('Done!!!')
    df1=df1.unique()
    df1.write_parquet(f'data//lhierarchy.parquet')

def query_st(loc='',reg='',prod='',fn='',pm=6,nm=6):
    query=f'''
    SELECT COUNT(1) FROM ( SELECT
        [SellingDivision] as [Selling Division],[COUNTRY_GROUP] 'Area',[StrykerGroupRegion] as [Stryker Group Region],[Region],[Country],p.[CatalogNumber],
        p.[Business_Sector] as [Business Sector],p.[Business_Unit] as [Business Unit],p.[Franchise],p.[Product_Line] as [Product Line],p.[IBP_Level_5] as [IBP Level 5],
        p.[IBP_Level_6] as [IBP Level 6],p.[IBP_Level_7] as [IBP Level 7],[SALES_DATE],p.[xx_uom_conversion] as UOM ,p.[PackContent] AS [Pack Content],
        SUM([L0_ASP_Final_Rev]) [`L0 ASP Final Rev], SUM([Act_Orders_Rev]) "`Act Orders Rev",
        SUM([Act_Orders_Rev_Val]) "Act Orders Rev Val", SUM(s.[L2_DF_Final_Rev]) as [L2 DF Final Rev],
        SUM(s."L1_DF_Final_Rev") as [L1 DF Final Rev], SUM(s."L0_DF_Final_Rev") as [L0 DF Final Rev],
        SUM(s.[L2_Stat_Final_Rev]) as [L2 Stat Final Rev], SUM(Fcst_DF_Final_Rev) as [`Fcst DF Final Rev], SUM(Fcst_Stat_Final_Rev) as [`Fcst Stat Final Rev],
        SUM(Fcst_Stat_Prelim_Rev) as [`Fcst Stat Prelim Rev], SUM(Fcst_DF_Final_Rev_Val) as [Fcst DF Final Rev Val]
        
    FROM [Envision].[Demantra_CLD_Fact_Sales] s

    JOIN [Envision].[DIM_Demantra_CLD_products] p
    ON s.item_skey = p.demantra_item_skey AND p.[Current] = 'True'
            
    JOIN [Envision].[DIM_Demantra_CLD_DemantraLocation] l
    ON s.Location_sKey = l.Location_skey

    JOIN [Envision].[Dim_DEMANTRA_CLD_MDP_Matrix] m
    ON s.MDP_Key = m.MDP_Key

    WHERE
        [SALES_DATE] BETWEEN DATEADD(month, {-pm}, GETDATE()) AND DATEADD(month, {nm}, GETDATE()) AND
        [{loc}] in ('{reg}') AND
        p.[{prod}] IN ('{fn}')
            
    GROUP BY
        [SellingDivision],[COUNTRY_GROUP],[StrykerGroupRegion],[Region],[Country],p.[Business_Sector],p.[Business_Unit],p.[Franchise],
        p.[IBP_Level_5],p.[IBP_Level_6],p.[IBP_Level_7],p.[Product_Line],[SALES_DATE],p.[CatalogNumber],p.[xx_uom_conversion],p.[PackContent]) AS subquery'''
    connection_string=f"Driver={{ODBC Driver 17 for SQL Server}};Server={ss};database=gda_glbsyndb;Encrypt=Yes;Authentication=ActiveDirectoryInteractive;"
    reader = read_arrow_batches_from_odbc(query=query,connection_string=connection_string)
    for batch in reader:
        #count= str(batch[0][0])+" rows to download  "
        count= str(batch[0][0])
    print(count+" rows to download  ")
    return count