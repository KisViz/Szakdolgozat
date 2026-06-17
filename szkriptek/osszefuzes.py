import pandas as pd
import numpy as np

# ---------------------------------------------------------
# 1. LÉPÉS: ALAPBEÁLLÍTÁSOK
# ---------------------------------------------------------
countries = ['HUN', 'DEU', 'AUT', 'POL', 'ESP', 'EST', 'EU27']
start_year = 1960
end_year = 2027

index_cols = pd.MultiIndex.from_product([countries, range(start_year, end_year + 1)],
                                        names=['Country', 'Year'])
master_df = pd.DataFrame(index=index_cols).reset_index()


# ---------------------------------------------------------
# 2. LÉPÉS: FÜGGVÉNYEK ADATBÁZISONKÉNT
# ---------------------------------------------------------

def process_world_bank_data(file_path, value_name):
    df = pd.read_csv(file_path, skiprows=4)

    # ÚJ: A Világbank EUU kódját átnevezzük EU27-re, hogy passzoljon a listánkhoz!
    df['Country Code'] = df['Country Code'].replace({'EUU': 'EU27'})

    # Szűrés országokra
    df = df[df['Country Code'].isin(countries)]

    years_cols = [str(y) for y in range(start_year, end_year + 1) if str(y) in df.columns]

    df_long = df.melt(id_vars=['Country Code'],
                      value_vars=years_cols,
                      var_name='Year',
                      value_name=value_name)

    df_long['Year'] = df_long['Year'].astype(int)
    df_long.rename(columns={'Country Code': 'Country'}, inplace=True)

    return df_long[['Country', 'Year', value_name]]


# ÚJ: Kiegészítettük a függvényt két paraméterrel, amikkel a pontos sorra szűrünk
def process_ameco_data(file_path, value_name, title_keyword, unit_keyword):
    df = pd.read_csv(file_path, sep=';', encoding='latin1')

    df['Country'] = df['CODE'].str.split('.').str[0]
    df = df[df['Country'].isin(countries)]

    # ÚJ: SZIGORÚ SZŰRÉS A CÍMRE ÉS A MÉRTÉKEGYSÉGRE
    # Ez biztosítja, hogy a 181 sorból csak az az 1 maradjon, ami valóban a GDP arányos adat
    df = df[df['TITLE'].str.contains(title_keyword, case=False, na=False)]
    df = df[df['UNIT'].str.contains(unit_keyword, case=False, na=False)]

    years_cols = [str(y) for y in range(start_year, end_year + 1) if str(y) in df.columns]

    df_long = df.melt(id_vars=['Country'],
                      value_vars=years_cols,
                      var_name='Year',
                      value_name=value_name)

    df_long['Year'] = df_long['Year'].astype(int)
    df_long[value_name] = pd.to_numeric(df_long[value_name], errors='coerce')

    # Ha esetleg így is maradna duplikáció (nem fog), ez a sor levédi a hibát
    df_long = df_long.drop_duplicates(subset=['Country', 'Year'])

    return df_long[['Country', 'Year', value_name]]


# ---------------------------------------------------------
# 3. LÉPÉS: A FÁJLOK BEOLVASÁSA (ÚJ PARAMÉTEREKKEL)
# ---------------------------------------------------------
print("Adatok beolvasása folyamatban...")

df_gdp = process_world_bank_data('../gdp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv', 'GDP_USD')
df_inflation = process_world_bank_data('../inflacio/API_FP.CPI.TOTL.ZG_DS2_en_csv_v2_175523.csv', 'Inflation_Rate')

# Megadjuk a kulcsszavakat, amik alapján kiszűrjük a sok felesleges sort az AMECO fájlokból
df_debt = process_ameco_data('../allamadossag/AMECO18.csv', 'Public_Debt_Pct',
                             title_keyword='gross debt',
                             unit_keyword='Percentage of GDP')

df_deficit = process_ameco_data('../allamhaztartas/AMECO16.csv', 'Budget_Deficit_Pct',
                                title_keyword='Net lending',
                                unit_keyword='Percentage of GDP')

# ---------------------------------------------------------
# 4. LÉPÉS: ÖSSZEFŰZÉS
# ---------------------------------------------------------
master_df = pd.merge(master_df, df_gdp, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_inflation, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_debt, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_deficit, on=['Country', 'Year'], how='left')

master_df.sort_values(by=['Country', 'Year'], inplace=True)

# ---------------------------------------------------------
# 5. LÉPÉS: MENTÉS ÉS ELLENŐRZÉSE
# ---------------------------------------------------------
print("\nAz összevont adatbázis első 10 sora:")
print(master_df.head(10))

master_df.to_csv('master_adatbazis_kesz.csv', index=False)
print("\nSikeres mentés: master_adatbazis_kesz.csv")