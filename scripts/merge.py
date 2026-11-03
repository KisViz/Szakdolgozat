import pandas as pd

# ---------------------------------------------------------
# 1. GLOBÁLIS BEÁLLÍTÁSOK ÉS CÉLSTRUKTÚRA INICIALIZÁLÁSA
# ---------------------------------------------------------
countries = ['HUN', 'DEU', 'POL']
start_year = 1960
end_year = 2027

# Többszintű index létrehozása Ország és Év kombinációjából (minden ország-év kombináció pontosan egy sorként szerepel)
index_cols = pd.MultiIndex.from_product([countries, range(start_year, end_year + 1)],
                                        names=['Country', 'Year'])
master_df = pd.DataFrame(index=index_cols).reset_index()


# ---------------------------------------------------------
# 2. FELDOLGOZÓ FÜGGVÉNYEK ADATBÁZISONKÉNT
# ---------------------------------------------------------
def process_world_bank_data(file_path, value_name):
    """A Világbank CSV fájljainak beolvasása és tisztítása."""
    df = pd.read_csv(file_path, skiprows=4)

    # Nomenklatúra egységesítése: EUU kód konvertálása EU27-re
    df['Country Code'] = df['Country Code'].replace({'EUU': 'EU27'})
    df = df[df['Country Code'].isin(countries)]

    years_cols = [str(y) for y in range(start_year, end_year + 1) if str(y) in df.columns]

    # "Wide-to-Long" transzformáció: Az év-oszlopok dimenzióvá alakítása
    df_long = df.melt(id_vars=['Country Code'],
                      value_vars=years_cols,
                      var_name='Year',
                      value_name=value_name)

    df_long['Year'] = df_long['Year'].astype(int)
    df_long.rename(columns={'Country Code': 'Country'}, inplace=True)

    return df_long[['Country', 'Year', value_name]]


def process_ameco_data(file_path, value_name, title_keyword, unit_keyword):
    """Az Európai Bizottság AMECO adatbázisának szemantikus szűrése és tisztítása."""
    df = pd.read_csv(file_path, sep=';', encoding='latin1')

    # Országkód kinyerése a komplex AMECO kódstruktúrából
    df['Country'] = df['CODE'].str.split('.').str[0]
    df = df[df['Country'].isin(countries)]

    # Szemantikus szűrés a megadott mutatókra és mértékegységekre
    df = df[df['TITLE'].str.contains(title_keyword, case=False, na=False)]
    df = df[df['UNIT'].str.contains(unit_keyword, case=False, na=False)]

    years_cols = [str(y) for y in range(start_year, end_year + 1) if str(y) in df.columns]

    # "Wide-to-Long" transzformáció
    df_long = df.melt(id_vars=['Country'],
                      value_vars=years_cols,
                      var_name='Year',
                      value_name=value_name)

    df_long['Year'] = df_long['Year'].astype(int)
    df_long[value_name] = pd.to_numeric(df_long[value_name], errors='coerce')
    df_long = df_long.drop_duplicates(subset=['Country', 'Year'])

    return df_long[['Country', 'Year', value_name]]


def process_oil_data(file_path, value_name):
    """A nyersolaj árak időbeli felbontásának szinkronizálása."""
    df = pd.read_csv(file_path, sep=';', decimal=',')

    # Dátumtípus konverziója és az év kinyerése
    df['Date'] = pd.to_datetime(df['Date'])
    df['Year'] = df['Date'].dt.year

    # Napi/havi adatok aggregálása éves átlagra (mean)
    df_yearly = df.groupby('Year')['Value'].mean().reset_index()
    df_yearly.rename(columns={'Value': value_name}, inplace=True)
    df_yearly = df_yearly[(df_yearly['Year'] >= start_year) & (df_yearly['Year'] <= end_year)]

    return df_yearly[['Year', value_name]]


# ---------------------------------------------------------
# 3. NYERS ADATOK BEOLVASÁSA ÉS TISZTÍTÁSA
# ---------------------------------------------------------
print("Adatok beolvasása folyamatban...")

df_gdp = process_world_bank_data('../gdp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv', 'GDP_USD')
df_inflation = process_world_bank_data('../inflation/API_FP.CPI.TOTL.ZG_DS2_en_csv_v2_175523.csv', 'Inflation_Rate')

df_debt = process_ameco_data('../gov_debt/AMECO18.csv', 'Public_Debt_Pct',
                             title_keyword='gross debt',
                             unit_keyword='Percentage of GDP')

df_deficit = process_ameco_data('../gov_deficit/AMECO16.csv', 'Budget_Deficit_Pct',
                                title_keyword='Net lending',
                                unit_keyword='Percentage of GDP')

df_oil = process_oil_data('../olaj/oil_prices.csv', 'Oil_Price_USD')

# ---------------------------------------------------------
# 4. ADATBÁZISOK EGYBEVONÁSA
# ---------------------------------------------------------
# Belső makromutatók összefűzése Ország és Év kulcsok alapján (Left Join)
master_df = pd.merge(master_df, df_gdp, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_inflation, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_debt, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_deficit, on=['Country', 'Year'], how='left')

# Exogén változó (olajár) összefűzése globálisan, kizárólag az Év kulcs alapján
master_df = pd.merge(master_df, df_oil, on='Year', how='left')

master_df.sort_values(by=['Country', 'Year'], inplace=True)

# ---------------------------------------------------------
# 5. EREDMÉNYEK MENTÉSE
# ---------------------------------------------------------
print("\nAz összevont adatbázis első 10 sora:")
print(master_df.head(10))

master_df.to_csv('../data/merged_data.csv', index=False)
print("\nSikeres mentés: merged_data.csv")