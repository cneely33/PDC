import pyodbc
import pandas as pd
 
# First setup a UserDSN in ODBC administrator for Teradata
connection = pyodbc.connect('DSN=EDW')
 
cursor = connection.cursor()  #passing method object to alais 
 
 
#table names:
pharmTable = "EDW_SEM_VIEWS.FV_Pharmacy_Claim_Line_Last_Status"
NDCTable = "EDW_SEM_VIEWS.DV_NDC"
DateTable = "EDW_SEM_VIEWS.DV_Claim_Filled_Date"
memberTable = "EDW_SEM_VIEWS.DV_Member_Current"
 
# Select a Persons first occurance of NDC Group Rx
a = "Member_Person_Identifier_Nbr \
    ,Days_Supply \
    "
b = ",Drug_TCC_Drug_Class \
    ,Drug_TCC_Drug_Subclass \
    ,Drug_TCC_Drug_Group \
    "
c = ",Claim_Filled_YYYY_MM_DD \
    "
    #End data isnt simply max; it can be crude estimate
 
           
sql = "SELECT {0} {1} {2} \
    FROM {3} \
    INNER JOIN {4}  \
    ON {3}.NDC_wk = {4}.NDC_wk \
    INNER JOIN {5} \
    ON {3}.Claim_Filled_Date_wk = {5}.Claim_Filled_Date_wk \
    INNER JOIN {6} \
    ON {3}.member_Wid = {6}.member_Wid \
    \
    WHERE Claim_Filled_YYYY_MM_DD > '2016-08-30' \
    ;".format(a, b, c, pharmTable, NDCTable, DateTable, memberTable)
 
df = pd.read_sql(sql,connection)
connection.close()
####Data Prep####
df['Claim_Filled_YYYY_MM_DD'] = pd.to_datetime(df['Claim_Filled_YYYY_MM_DD'])
 
# Rx counts will be grouped by member and drug group/class/subclass
groupby = [df['Member_Person_Identifier_Nbr'],df['Drug_TCC_Drug_Group'],
            df['Drug_TCC_Drug_Class'], df['Drug_TCC_Drug_Subclass']]
 
########## Create count of days supply of medication by group ###########
dfSum = df.groupby(groupby)['Days_Supply'].sum()
supply = []
for i, row in df.iterrows():
    #list OfTupes is a list with a tuple to match to total supply
    listOfTupes = [(df['Member_Person_Identifier_Nbr'][i],
                 df['Drug_TCC_Drug_Group'][i],
                 df['Drug_TCC_Drug_Class'][i],
                 df['Drug_TCC_Drug_Subclass'][i]
                 )]
    #lookup the total supply based off the index name
    supply.append(dfSum.loc[listOfTupes[0]])
 
df['Sum_DaysSupply'] = supply
 
######Create a series with the Last/First day a member filled a particular drug class
    #testing lambda with function to grab the Max of Claim Filled Date
    # last/firstfillD are series with indexes as tuples based off of the group by vars
lastFillD = df.groupby(groupby)['Claim_Filled_YYYY_MM_DD'].apply(lambda x: x.max())
firstFillD = df.groupby(groupby)['Claim_Filled_YYYY_MM_DD'].min()
 
#assign the last fill date for each Member and Drug Class combo
fill = []
for row in range(len(df['Member_Person_Identifier_Nbr'])): #slower way;
                                                # can be used for direct edits
    #list OfTupes is a list with a tuple to match to the first filled date
    listOfTupes = [(df['Member_Person_Identifier_Nbr'][row],
                 df['Drug_TCC_Drug_Group'][row],
                 df['Drug_TCC_Drug_Class'][row],
                 df['Drug_TCC_Drug_Subclass'][row]
                 )]
    #lookup the first fill date based off the index name
    fill.append(firstFillD.loc[listOfTupes[0]])
 
df['FirstFillDay'] = fill
 
fill = []
for i, row in df.iterrows():
    #list OfTupes is a list with a tuple to match to the last filled date
    listOfTupes = [(df['Member_Person_Identifier_Nbr'][i],
                 df['Drug_TCC_Drug_Group'][i],
                 df['Drug_TCC_Drug_Class'][i],
                 df['Drug_TCC_Drug_Subclass'][i]
                 )]
    #lookup the last fill date based off the index name
    fill.append(lastFillD.loc[listOfTupes[0]])
 
df['LastFillDay'] = fill
 
###### Create Last Fill Amount ####
    ## This accounts for multiple scripts on last fill day
supplyGroup = df.groupby(groupby)

# conditional sum of days supply
supplyFinal = supplyGroup.apply(lambda x: x[x['Claim_Filled_YYYY_MM_DD'] == x['LastFillDay']]['Days_Supply'].sum())
supply = []
for i, row in df.iterrows():
    listOfTupes = [(df['Member_Person_Identifier_Nbr'][i],
                 df['Drug_TCC_Drug_Group'][i],
                 df['Drug_TCC_Drug_Class'][i],
                 df['Drug_TCC_Drug_Subclass'][i]
                 )]
    supply.append(supplyFinal.loc[listOfTupes[0]])
 
df['LastFillSupply'] = supply
 
###### Subtract the last filled days supply from total supply #####
df['FinalSupply'] = df['Sum_DaysSupply'] - df['LastFillSupply']
 
###### Calculate the days between first and last fill date    #####
df['DaysCovered'] = df['LastFillDay'] - df['FirstFillDay']
    #convert datetime var to count of days int
df.DaysCovered = df.DaysCovered.dt.days
 
###### Remove values with only one fill date (the first fill) #####
for i, row in df.iterrows():
    if df['LastFillDay'][i] == df['FirstFillDay'][i]:
        df.drop([i], inplace=True)
 
###### Drop Columns and Rows that are not needed #####
df01 = df.drop(df.columns[[1,5,6,7,8,9]], axis=1)
df01 = df01.drop_duplicates()
 
##### Calculate Portion of Days Covered #####
df01['PDC'] = df01['FinalSupply'] / df01['DaysCovered']
df01 = df01.reset_index(drop=True)
 
 
