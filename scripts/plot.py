import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS BEÁLLÍTÁSOK
# ---------------------------------------------------------

# Létrehozzuk a kimeneti mappát, ha még nem létezne
output_dir = '../pictures'
os.makedirs(output_dir, exist_ok=True)

print("Adatok beolvasása...")
# Figyelj az elérési útvonalra, feltételezem, hogy az arima.csv is a data mappában van
df = pd.read_csv('../data/merged_data.csv')
df_arima = pd.read_csv('../data/arima.csv')

# Szebb, szakdolgozatos stílus beállítása
sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)

# ---------------------------------------------------------
# 2. ÁBRA: GDP ALAKULÁSA (Vonaldiagram logaritmikus skálán)
# ---------------------------------------------------------
print("GDP ábra generálása...")
plt.figure(figsize=(10, 6))

# A GDP-t vonaldiagramon ábrázoljuk.
sns.lineplot(data=df, x='Year', y='GDP_USD', hue='Country', linewidth=2)

plt.title('A nominális GDP alakulása (1960 - 2025)', fontsize=14, pad=15)
plt.ylabel('GDP (USD - Logaritmikus skála)')
plt.xlabel('Év')

# Logaritmikus skálát használunk, mert a német/EU27 GDP sokkal nagyobb, mint a magyar vagy észt.
plt.yscale('log')
plt.xlim(1990, 2024)

plt.tight_layout()
# MENTÉS AZ ÚJ MAPPÁBA
plt.savefig(f'{output_dir}/gdp_trend.png', dpi=300)
plt.close()  # memóriafelszabadítás a következő ábra előtt

# ---------------------------------------------------------
# 3. ÁBRA: INFLÁCIÓ - MAGYARORSZÁG VS. EU27
# ---------------------------------------------------------
print("Infláció ábra generálása...")
plt.figure(figsize=(10, 5))

df_inf = df[df['Country'].isin(['HUN', 'EU27'])]

sns.lineplot(data=df_inf, x='Year', y='Inflation_Rate', hue='Country',
             palette=['#d62728', '#1f77b4'], linewidth=2.5)

plt.title('Inflációs ráta: Magyarország vs. EU27 átlag', fontsize=14, pad=15)
plt.ylabel('Infláció (%)')
plt.xlabel('Év')
plt.xlim(1995, 2024)

plt.axhline(0, color='grey', linestyle='--', linewidth=1)

plt.tight_layout()
# MENTÉS AZ ÚJ MAPPÁBA
plt.savefig(f'{output_dir}/inflacio_hun_vs_eu27.png', dpi=300)
plt.close()

# ---------------------------------------------------------
# 4. ÁBRA: ÁLLAMADÓSSÁG VS. KÖLTSÉGVETÉSI HIÁNY (Pontdiagram - Scatter)
# ---------------------------------------------------------
vizsgalt_ev = 2022
print(f"Keresztmetszeti ábra generálása a {vizsgalt_ev}. évre...")

plt.figure(figsize=(9, 7))

df_ev = df[df['Year'] == vizsgalt_ev].dropna(subset=['Public_Debt_Pct', 'Budget_Deficit_Pct'])

sns.scatterplot(data=df_ev, x='Budget_Deficit_Pct', y='Public_Debt_Pct',
                hue='Country', s=150, palette='Set2')

for i in range(df_ev.shape[0]):
    plt.text(x=df_ev['Budget_Deficit_Pct'].iloc[i] + 0.2,
             y=df_ev['Public_Debt_Pct'].iloc[i] + 0.2,
             s=df_ev['Country'].iloc[i],
             fontsize=10, weight='bold')

plt.title(f'Államadósság és Költségvetési egyenleg ({vizsgalt_ev})', fontsize=14, pad=15)
plt.xlabel('Költségvetési egyenleg (GDP %)')
plt.ylabel('Államadósság (GDP %)')

plt.axvline(0, color='red', linestyle='--', linewidth=1, alpha=0.5)
plt.axhline(60, color='red', linestyle='--', linewidth=1, alpha=0.5)

plt.tight_layout()
# MENTÉS AZ ÚJ MAPPÁBA
plt.savefig(f'{output_dir}/adossag_vs_egyenleg_{vizsgalt_ev}.png', dpi=300)
plt.close()

# ---------------------------------------------------------
# 5. ÁBRA (ÚJ): TÉNYLEGES VS. ELŐREJELZETT (ARIMA) ADATOK
# ---------------------------------------------------------
vizsgalt_orszag = 'HUN'
print(f"What-if előrejelzés összehasonlító ábra generálása ({vizsgalt_orszag})...")

# Létrehozunk egy 2x2-es ábra rácsot (4 mutatóhoz)
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle(f'{vizsgalt_orszag}: Tényadatok vs. COVID nélküli ARIMA előrejelzés', fontsize=16, weight='bold', y=0.98)

# Szűrjük az adatokat a vizsgált országra, és kiválasztjuk a releváns időszakot (pl. 2010-től)
df_aktualis = df[(df['Country'] == vizsgalt_orszag) & (df['Year'] >= 2010)]
df_predikcio = df_arima[(df_arima['Country'] == vizsgalt_orszag)]

# A 4 vizsgálandó mutató és a grafikonok elrendezése
mutatok = [
    ('GDP_USD', 'GDP (USD)', axes[0, 0]),
    ('Inflation_Rate', 'Infláció (%)', axes[0, 1]),
    ('Public_Debt_Pct', 'Államadósság (GDP %)', axes[1, 0]),
    ('Budget_Deficit_Pct', 'Költségvetési egyenleg (GDP %)', axes[1, 1])
]

for col_name, title, ax in mutatok:
    # Valós adatok rajzolása (folytonos kék vonal)
    sns.lineplot(data=df_aktualis, x='Year', y=col_name, ax=ax, label='Tényadatok', color='#1f77b4', linewidth=2.5)

    # ARIMA előrejelzés rajzolása (szaggatott narancs vonal)
    sns.lineplot(data=df_predikcio, x='Year', y=col_name, ax=ax, label='ARIMA trend (Válság nélkül)', color='#ff7f0e',
                 linestyle='--', linewidth=2.5)

    # Külalak beállítások
    ax.set_title(title, fontsize=12)
    ax.set_xlabel('Év')
    ax.set_ylabel('')

    # 2020 (a válság és az előrejelzés kezdete) jelölése egy függőleges vonallal
    ax.axvline(2020, color='red', linestyle=':', alpha=0.7, label='2020 (Töréspont)')
    ax.legend(loc='best')

plt.tight_layout()
# Kisebb igazítás, hogy a főcím (suptitle) ne csússzon rá az ábrákra
plt.subplots_adjust(top=0.92)
plt.savefig(f'{output_dir}/what_if_elorejelzes_{vizsgalt_orszag}.png', dpi=300)