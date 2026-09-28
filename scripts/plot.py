import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS BEÁLLÍTÁSOK
# ---------------------------------------------------------
output_dir = '../pictures'
os.makedirs(output_dir, exist_ok=True)

print("Adatok beolvasása...")
df = pd.read_csv('../data/merged_data.csv')
df_arima = pd.read_csv('../data/arima_scenarios.csv')

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)

# ---------------------------------------------------------
# (1-4. ÁBRA): TÖRTÉNELMI ADATOK (Sima /pictures mappába)
# ---------------------------------------------------------
print("Alap diagramok generálása...")

# 2. ÁBRA: GDP
plt.figure(figsize=(10, 6))
sns.lineplot(data=df, x='Year', y='GDP_USD', hue='Country', linewidth=2)
plt.title('A nominális GDP alakulása (1960 - 2025)', fontsize=14, pad=15)
plt.ylabel('GDP (USD - Logaritmikus skála)')
plt.xlabel('Év')
plt.yscale('log')
plt.xlim(1990, 2024)
plt.tight_layout()
plt.savefig(f'{output_dir}/gdp_trend.png', dpi=300)
plt.close()

# 3. ÁBRA: INFLÁCIÓ
plt.figure(figsize=(10, 5))
df_inf = df[df['Country'].isin(['HUN', 'EU27'])]
sns.lineplot(data=df_inf, x='Year', y='Inflation_Rate', hue='Country', palette=['#d62728', '#1f77b4'], linewidth=2.5)
plt.title('Inflációs ráta: Magyarország vs. EU27 átlag', fontsize=14, pad=15)
plt.ylabel('Infláció (%)')
plt.xlabel('Év')
plt.xlim(1995, 2024)
plt.axhline(0, color='grey', linestyle='--', linewidth=1)
plt.tight_layout()
plt.savefig(f'{output_dir}/inflacio_hun_vs_eu27.png', dpi=300)
plt.close()

# 4. ÁBRA: SCATTERPLOT
vizsgalt_ev = 2022
plt.figure(figsize=(9, 7))
df_ev = df[df['Year'] == vizsgalt_ev].dropna(subset=['Public_Debt_Pct', 'Budget_Deficit_Pct'])
sns.scatterplot(data=df_ev, x='Budget_Deficit_Pct', y='Public_Debt_Pct', hue='Country', s=150, palette='Set2')
for i in range(df_ev.shape[0]):
    plt.text(x=df_ev['Budget_Deficit_Pct'].iloc[i] + 0.2, y=df_ev['Public_Debt_Pct'].iloc[i] + 0.2,
             s=df_ev['Country'].iloc[i], fontsize=10, weight='bold')
plt.title(f'Államadósság és Költségvetési egyenleg ({vizsgalt_ev})', fontsize=14, pad=15)
plt.xlabel('Költségvetési egyenleg (GDP %)')
plt.ylabel('Államadósság (GDP %)')
plt.axvline(0, color='red', linestyle='--', linewidth=1, alpha=0.5)
plt.axhline(60, color='red', linestyle='--', linewidth=1, alpha=0.5)
plt.tight_layout()
plt.savefig(f'{output_dir}/adossag_vs_egyenleg_{vizsgalt_ev}.png', dpi=300)
plt.close()

# ---------------------------------------------------------
# 5. ÁBRA (ÚJ): ALMAPPÁS MENTÉS PARAMÉTERENKÉNT
# ---------------------------------------------------------
countries_to_plot = df['Country'].unique()
scenarios = df_arima['Scenario'].unique()
# Kinyerjük az összes lefuttatott ARIMA paramétert a CSV-ből
model_orders = df_arima['Model_Order'].unique()

print(f"\nWhat-if ábrák generálása {len(model_orders)} különböző paraméterbeállításhoz...")

for order in model_orders:
    # Létrehozzuk a paraméter saját mappáját (pl. ../pictures/ARIMA_1_1_1)
    order_dir = f'{output_dir}/ARIMA_{order}'
    os.makedirs(order_dir, exist_ok=True)

    print(f"\n>>> Generálás a mappába: {order_dir}")

    for vizsgalt_orszag in countries_to_plot:
        for scenario in scenarios:
            toreSpont_ev = int(scenario.split('_')[0])
            kezdo_ev_abrazolas = toreSpont_ev - 10

            fig, axes = plt.subplots(2, 2, figsize=(14, 10))

            # Formázott ARIMA név a címbe (pl. 1_1_1 -> 1,1,1)
            formatted_order = order.replace('_', ',')
            fig.suptitle(f'{vizsgalt_orszag}: Tényadatok vs. ARIMA({formatted_order}) [{scenario.replace("_", " ")}]',
                         fontsize=16, weight='bold', y=0.98)

            # Szűrés országra ÉS a konkrét ARIMA paraméterre!
            df_aktualis = df[(df['Country'] == vizsgalt_orszag) & (df['Year'] >= kezdo_ev_abrazolas)]
            df_predikcio = df_arima[(df_arima['Country'] == vizsgalt_orszag) &
                                    (df_arima['Scenario'] == scenario) &
                                    (df_arima['Model_Order'] == order)]

            mutatok = [
                ('GDP_USD', 'GDP (USD)', axes[0, 0]),
                ('Inflation_Rate', 'Infláció (%)', axes[0, 1]),
                ('Public_Debt_Pct', 'Államadósság (GDP %)', axes[1, 0]),
                ('Budget_Deficit_Pct', 'Költségvetési egyenleg (GDP %)', axes[1, 1])
            ]

            for col_name, title, ax in mutatok:
                sns.lineplot(data=df_aktualis, x='Year', y=col_name, ax=ax, label='Tényadatok', color='#1f77b4',
                             linewidth=2.5)
                sns.lineplot(data=df_predikcio, x='Year', y=col_name, ax=ax, label=f'ARIMA({formatted_order}) trend',
                             color='#ff7f0e', linestyle='--', linewidth=2.5)

                ax.set_title(title, fontsize=12)
                ax.set_xlabel('Év')
                ax.set_ylabel('')
                ax.axvline(toreSpont_ev, color='red', linestyle=':', alpha=0.7, label=f'{toreSpont_ev} (Töréspont)')
                ax.legend(loc='best')

            plt.tight_layout()
            plt.subplots_adjust(top=0.92)

            # MENTÉS AZ ALMAPPÁBA
            plt.savefig(f'{order_dir}/what_if_{vizsgalt_orszag}_{scenario}.png', dpi=300)
            plt.close()

print(f"\nMinden almappa és ábra sikeresen elkészült!")