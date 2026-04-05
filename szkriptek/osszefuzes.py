from idlelib.iomenu import encoding
import pandas as pd
import numpy as np

# ---------------------------------------------------------
# 1. LÉPÉS: ALAPBEÁLLÍTÁSOK (Országok és időtáv)
# ---------------------------------------------------------

# Használjuk az ISO-3 országkódokat, mert a legtöbb nemzetközi adatbázis ezt ismeri
countries = ['HUN', 'DEU', 'AUT', 'POL', 'ESP', 'EST', 'EU27']
start_year = 1960
end_year = 2027

# Létrehozzuk a "Mester" tábla vázát.
# Ez generál egy listát minden évhez és minden országhoz (panel adatstruktúra).
index_cols = pd.MultiIndex.from_product([countries, range(start_year, end_year + 1)],
                                        names=['Country', 'Year'])
master_df = pd.DataFrame(index=index_cols).reset_index()


# ---------------------------------------------------------
# 2. LÉPÉS: FÜGGVÉNYEK AZ ADATOK BEOLVASÁSÁRA ÉS TISZTÍTÁSÁRA
# ---------------------------------------------------------

def process_world_bank_data(file_path, value_name):
    """
    A Világbankos CSV-k beolvasása és formázása.
    A Világbanknál az évek oszlopként (széles formátumban) szerepelnek, ezt alakítjuk át.
    """
    # A Világbank exportjában általában az első 4 sor felesleges fejléc
    df = pd.read_csv(file_path, skiprows=4)

    # Szűrünk az ISO-3 országkódokra
    df = df[df['Country Code'].isin(countries)]

    # "Melt" művelet: Az oszlopokká tett éveket egymás alatti sorokká alakítjuk (Long format)
    years_cols = [str(y) for y in range(start_year, end_year + 1) if str(y) in df.columns]

    df_long = df.melt(id_vars=['Country Code'],
                      value_vars=years_cols,
                      var_name='Year',
                      value_name=value_name)

    # Az évek oszlopot számmá (integer) alakítjuk a könnyebb párosítás miatt
    df_long['Year'] = df_long['Year'].astype(int)

    # Oszlop átnevezése a Mester táblához való illesztéshez
    df_long.rename(columns={'Country Code': 'Country'}, inplace=True)

    return df_long[['Country', 'Year', value_name]]


def process_ameco_data(file_path, value_name):
    """
    Kifejezetten az AMECO adatbázis feldolgozására írt függvény.
    Kezeli a latin1 kódolást, a pontosvesszőt, és a wide -> long transzformációt.
    """
    # 1. Beolvasás a megfelelő kódolással és elválasztóval
    df = pd.read_csv(file_path, sep=';', encoding='latin1')

    # 2. Országkód kinyerése
    # A CODE oszlopból (pl. EU27.1.0.0.0.UDGG) a split segítségével levágjuk a pont előtti részt
    df['Country'] = df['CODE'].str.split('.').str[0]

    # 3. Szűrés a mi mintánkra
    df = df[df['Country'].isin(countries)]

    # 4. Év oszlopok dinamikus azonosítása
    # Ez azért is jó, mert az utolsó, üres oszlopot (amit a sor végi pontosvessző okoz) automatikusan figyelmen kívül hagyja
    years_cols = [str(y) for y in range(start_year, end_year + 1) if str(y) in df.columns]

    # 5. Wide -> Long átalakítás (hogy egymás alá kerüljenek az évek)
    df_long = df.melt(id_vars=['Country'],
                      value_vars=years_cols,
                      var_name='Year',
                      value_name=value_name)

    # 6. Típuskonverziók és tisztítás
    df_long['Year'] = df_long['Year'].astype(int)

    # A pandas to_numeric függvénye az 'NA' és egyéb szöveges hiányzó értékeket
    # automatikusan NaN (Not a Number) formátumra cseréli, a számokat pedig float-tá alakítja
    df_long[value_name] = pd.to_numeric(df_long[value_name], errors='coerce')

    # Csak a letisztított, esszenciális 3 oszlopot adjuk vissza
    return df_long[['Country', 'Year', value_name]]


# ---------------------------------------------------------
# 3. LÉPÉS: A FÁJLOK BEOLVASÁSA
# ---------------------------------------------------------
# FIGYELEM: A fájlneveket ('letoltott_...csv') cseréld le arra a névre, ahogy elmentetted őket!

print("Adatok beolvasása folyamatban...")

# Világbank adatok
df_gdp = process_world_bank_data('../gdp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_133326.csv', 'GDP_USD')
df_inflation = process_world_bank_data('../inflacio/API_FP.CPI.TOTL.ZG_DS2_en_csv_v2_175523.csv', 'Inflation_Rate')

# AMECO / Eurostat adatok (Példa hívások, ellenőrizd az oszlopneveket a megnyitott CSV-ben!)
df_debt = process_ameco_data('../allamadossag/AMECO18.csv', 'Public_Debt_Pct')
df_deficit = process_ameco_data('../allamhaztartas/AMECO16.csv', 'Budget_Deficit_Pct')

# ---------------------------------------------------------
# 4. LÉPÉS: ADATBÁZISOK ÖSSZEFŰZÉSE (MERGE)
# ---------------------------------------------------------

# Szépen sorban hozzácsatoljuk a letisztított táblákat a Mester táblához az Ország és Év alapján
# (Vedd ki a komment jeleket (#), ha már beolvastad a fájlokat)

master_df = pd.merge(master_df, df_gdp, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_inflation, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_debt, on=['Country', 'Year'], how='left')
master_df = pd.merge(master_df, df_deficit, on=['Country', 'Year'], how='left')

# Rendezés, hogy szépen áttekinthető legyen
master_df.sort_values(by=['Country', 'Year'], inplace=True)

# ---------------------------------------------------------
# 5. LÉPÉS: AZ EREDMÉNY MENTÉSE ÉS ELLENŐRZÉSE
# ---------------------------------------------------------

print("\nAz összevont adatbázis első 10 sora:")
print(master_df.head(10))

# Elmentjük az összeállt adatbázist, hogy ezen végezzük majd a "mi lett volna ha" modellezést
master_df.to_csv('master_adatbazis_kesz.csv', index=False)
print("\nSikeres mentés: master_adatbazis_kesz.csv")