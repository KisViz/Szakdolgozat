import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import numpy as np

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS BEÁLLÍTÁSOK
# ---------------------------------------------------------
output_dir = '../pictures'
os.makedirs(output_dir, exist_ok=True)

print("Adatok beolvasása...")
df_actual = pd.read_csv('../data/merged_data.csv')

# Mind a 3 modell kimenetét külön olvassuk be
df_arima = pd.read_csv('../data/arima_scenarios.csv')
df_arimax = pd.read_csv('../data/arimax_scenarios.csv')
df_var = pd.read_csv('../data/var_scenarios.csv')

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)
countries_to_plot = df_actual['Country'].unique()
indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']

# ---------------------------------------------------------
# (1-4. ÁBRA): TÖRTÉNELMI ADATOK
# ---------------------------------------------------------
print("Alap diagramok generálása...")

# 2. ÁBRA: GDP
plt.figure(figsize=(10, 6))
sns.lineplot(data=df_actual, x='Year', y='GDP_USD', hue='Country', linewidth=2)
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
df_inf = df_actual[df_actual['Country'].isin(['HUN', 'EU27'])]
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
df_ev = df_actual[df_actual['Year'] == vizsgalt_ev].dropna(subset=['Public_Debt_Pct', 'Budget_Deficit_Pct'])
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
# Y-TENGELY SKÁLÁK EGYSÉGESÍTÉSE (Közös Min/Max keresése)
# ---------------------------------------------------------
print("Y-tengely skálák egységesítése országonként és mutatónként...")
y_limits = {}
df_all_models = pd.concat([df_arima, df_arimax, df_var])

for c in countries_to_plot:
    y_limits[c] = {}
    for ind in indicators:
        # Tényadatok minimuma és maximuma 2010-től
        act_min = df_actual[(df_actual['Country'] == c) & (df_actual['Year'] >= 2010)][ind].min()
        act_max = df_actual[(df_actual['Country'] == c) & (df_actual['Year'] >= 2010)][ind].max()

        # Modellek (ARIMA, ARIMAX, VAR) minimuma és maximuma
        pred_min = df_all_models[df_all_models['Country'] == c][ind].min()
        pred_max = df_all_models[df_all_models['Country'] == c][ind].max()

        overall_min = min(act_min, pred_min) if pd.notna(act_min) and pd.notna(pred_min) else act_min
        overall_max = max(act_max, pred_max) if pd.notna(act_max) and pd.notna(pred_max) else act_max

        # 10% margó hozzáadása a grafikon széléhez
        padding = (overall_max - overall_min) * 0.1 if pd.notna(overall_max) else 0
        y_limits[c][ind] = (overall_min - padding, overall_max + padding)


# ---------------------------------------------------------
# 5. ÁBRA: UNIVERZÁLIS FÜGGVÉNY A WHAT-IF MODELLEKHEZ
# ---------------------------------------------------------
def generate_model_plots(df_model, model_base_name, extra_title=""):
    model_orders = df_model['Model_Order'].unique()
    scenarios = df_model['Scenario'].unique()

    for order in model_orders:
        if model_base_name == "VAR":
            order_dir = f'{output_dir}/{model_base_name}_{order}'
        else:
            order_dir = f'{output_dir}/{model_base_name}_{order}'

        os.makedirs(order_dir, exist_ok=True)
        print(f">>> Generálás a mappába: {order_dir}")

        for vizsgalt_orszag in countries_to_plot:
            for scenario in scenarios:
                toreSpont_ev = int(scenario.split('_')[0])
                kezdo_ev_abrazolas = toreSpont_ev - 10

                formatted_order = order.replace('_', ',') if model_base_name != "VAR" else order

                df_akt = df_actual[
                    (df_actual['Country'] == vizsgalt_orszag) & (df_actual['Year'] >= kezdo_ev_abrazolas)]
                df_pred = df_model[(df_model['Country'] == vizsgalt_orszag) &
                                   (df_model['Scenario'] == scenario) &
                                   (df_model['Model_Order'] == order)]

                # Most már nincsenek axes rácsok, csak a nevek
                mutatok = [
                    ('GDP_USD', 'GDP (USD)'),
                    ('Inflation_Rate', 'Infláció (%)'),
                    ('Public_Debt_Pct', 'Államadósság (GDP %)'),
                    ('Budget_Deficit_Pct', 'Költségvetési egyenleg (GDP %)')
                ]

                # Minden mutatónál külön képet generálunk
                for col_name, title in mutatok:
                    plt.figure(figsize=(10, 6))
                    ax = plt.gca()  # Lekérjük az aktuális tengelyt

                    # Dinamikus cím minden egyes képhez
                    plt.title(
                        f'{vizsgalt_orszag}: Tényadatok vs. {model_base_name}({formatted_order}) {extra_title}[{scenario.replace("_", " ")}]\n{title}',
                        fontsize=14, weight='bold', pad=15)

                    sns.lineplot(data=df_akt, x='Year', y=col_name, ax=ax, label='Tényadatok', color='#1f77b4',
                                 linewidth=2.5)
                    sns.lineplot(data=df_pred, x='Year', y=col_name, ax=ax, label=f'{model_base_name} trend',
                                 color='#ff7f0e', linestyle='--', linewidth=2.5)

                    ax.set_xlabel('Év')
                    ax.set_ylabel('')

                    # EGYSÉGES SKÁLA BEÁLLÍTÁSA
                    if pd.notna(y_limits[vizsgalt_orszag][col_name][0]):
                        ax.set_ylim(y_limits[vizsgalt_orszag][col_name])

                    ax.axvline(toreSpont_ev, color='red', linestyle=':', alpha=0.7, label=f'{toreSpont_ev} (Töréspont)')
                    ax.legend(loc='best')

                    plt.tight_layout()

                    # Fájlnév: what_if_HUN_2020_Covid19_GDP_USD.png az adott modell mappájában
                    plt.savefig(f'{order_dir}/what_if_{vizsgalt_orszag}_{scenario}_{col_name}.png', dpi=300)
                    plt.close()


# ---------------------------------------------------------
# FÜGGVÉNY HÍVÁSA MINDEGYIK MODELLRE
# ---------------------------------------------------------
print("\n--- ARIMA diagramok rajzolása ---")
generate_model_plots(df_arima, "ARIMA")

print("\n--- ARIMAX diagramok rajzolása ---")
generate_model_plots(df_arimax, "ARIMAX", extra_title="(Külső: Olajár) ")

print("\n--- VAR diagramok rajzolása ---")
generate_model_plots(df_var, "VAR")

print(f"\nMinden almappa és ábra sikeresen elkészült!")