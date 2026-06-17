import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS BEÁLLÍTÁSOK
# ---------------------------------------------------------

print("Adatok beolvasása...")
df = pd.read_csv('master_adatbazis_kesz.csv')

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

# Logaritmikus skálát használunk, mert a német/EU27 GDP sokkal nagyobb, mint a magyar vagy észt,
# és sima skálán a kisebb országok vonalai teljesen egybeolvadnának alul.
plt.yscale('log')

# X tengely határainak beállítása (pl. csak 1990-től nézzük)
plt.xlim(1990, 2024)

plt.tight_layout()
plt.savefig('gdp_trend.png', dpi=300) # dpi=300 a nagy felbontásért
plt.show()

# ---------------------------------------------------------
# 3. ÁBRA: INFLÁCIÓ - MAGYARORSZÁG VS. EU27
# ---------------------------------------------------------
print("Infláció ábra generálása...")
plt.figure(figsize=(10, 5))

# Szűrünk csak a magyar és az EU átlag adatokra
df_inf = df[df['Country'].isin(['HUN', 'EU27'])]

sns.lineplot(data=df_inf, x='Year', y='Inflation_Rate', hue='Country',
             palette=['#d62728', '#1f77b4'], linewidth=2.5) # Piros és kék színek

plt.title('Inflációs ráta: Magyarország vs. EU27 átlag', fontsize=14, pad=15)
plt.ylabel('Infláció (%)')
plt.xlabel('Év')
plt.xlim(1995, 2024) # 1995 utáni időszak

# Húzunk egy szürke szaggatott vonalat a 0%-hoz (defláció határa)
plt.axhline(0, color='grey', linestyle='--', linewidth=1)

plt.tight_layout()
plt.savefig('inflacio_hun_vs_eu27.png', dpi=300)
plt.show()

# ---------------------------------------------------------
# 4. ÁBRA: ÁLLAMADÓSSÁG VS. KÖLTSÉGVETÉSI HIÁNY (Pontdiagram - Scatter)
# ---------------------------------------------------------
# Itt egy adott évet (pl. 2022) vizsgálunk keresztmetszetben
vizsgalt_ev = 2022
print(f"Keresztmetszeti ábra generálása a {vizsgalt_ev}. évre...")

plt.figure(figsize=(9, 7))

# Kiszűrjük a 2022-es évet, és eldobjuk azokat a sorokat, ahol nincs adat
df_ev = df[df['Year'] == vizsgalt_ev].dropna(subset=['Public_Debt_Pct', 'Budget_Deficit_Pct'])

sns.scatterplot(data=df_ev, x='Budget_Deficit_Pct', y='Public_Debt_Pct',
                hue='Country', s=150, palette='Set2') # s=150 a pontok mérete

# Ráírjuk az országkódokat a pontok mellé, hogy tudjuk melyik pötty kihez tartozik
for i in range(df_ev.shape[0]):
    plt.text(x=df_ev['Budget_Deficit_Pct'].iloc[i] + 0.2,
             y=df_ev['Public_Debt_Pct'].iloc[i] + 0.2,
             s=df_ev['Country'].iloc[i],
             fontsize=10, weight='bold')

plt.title(f'Államadósság és Költségvetési egyenleg ({vizsgalt_ev})', fontsize=14, pad=15)
plt.xlabel('Költségvetési egyenleg (GDP %)')
plt.ylabel('Államadósság (GDP %)')

# Referencia vonalak: 0%-os egyenleg és pl. 60%-os maastrichti adósságkorlát
plt.axvline(0, color='red', linestyle='--', linewidth=1, alpha=0.5)
plt.axhline(60, color='red', linestyle='--', linewidth=1, alpha=0.5)

plt.tight_layout()
plt.savefig(f'adossag_vs_egyenleg_{vizsgalt_ev}.png', dpi=300)
plt.show()

print("Minden ábra sikeresen elkészült és mentve lett a mappába!")