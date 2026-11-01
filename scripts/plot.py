import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS KÖRNYEZET BEÁLLÍTÁSA
# ---------------------------------------------------------
output_dir = '../pictures'
os.makedirs(output_dir, exist_ok=True)

print("Adatok beolvasása...")
df_actual = pd.read_csv('../data/merged_data.csv')

# Mind a 3 szimulációs kimenet beolvasása az összehasonlításhoz
df_arima = pd.read_csv('../data/arima_scenarios.csv')
df_arimax = pd.read_csv('../data/arimax_scenarios.csv')
df_var = pd.read_csv('../data/var_scenarios.csv')

sns.set_theme(style="whitegrid", context="paper", font_scale=1.2)
countries_to_plot = df_actual['Country'].unique()
indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']


# ---------------------------------------------------------
# 2. ALAP DIAGRAMOK GENERÁLÁSA (TÖRTÉNELMI ADATOK)
# ---------------------------------------------------------
print("Alap diagramok generálása...")

# 2.1. ÁBRA: GDP trend alakulása (Logaritmikus skála)
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

# 2.2. ÁBRA: Inflációs ráta összehasonlítása (HUN vs. EU27)
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

# 2.3. ÁBRA: Államadósság és Deficit eloszlása (Scatterplot, 2022)
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

# 2.4. ÁBRA: Magyar GDP zuhanás (2018-2021)
print("Magyar GDP (2018-2021) ábra generálása...")
plt.figure(figsize=(10, 6))
df_gdp_covid = df_actual[(df_actual['Country'].isin(['HUN'])) &
                         (df_actual['Year'] >= 2018) & (df_actual['Year'] <= 2021)]
sns.lineplot(data=df_gdp_covid, x='Year', y='GDP_USD', hue='Country', linewidth=3, marker='o', markersize=8)
plt.title('A „Hirtelen leállás”: GDP alakulása (2018 - 2021)', fontsize=14, pad=15)
plt.ylabel('GDP (USD - Logaritmikus skála)')
plt.xlabel('Év')
plt.yscale('log')
plt.xticks([2018, 2019, 2020, 2021])
plt.tight_layout()
plt.savefig(f'{output_dir}/gdp_covid_hun_2018_2021.png', dpi=300)
plt.close()

# 2.5. ÁBRA: Stagfláció (Dupla tengelyes grafikon, 2021-2023)
print("Stagfláció dupla tengelyes ábra generálása...")
df_stag = df_actual[(df_actual['Country'] == 'HUN') & (df_actual['Year'] >= 2021) & (df_actual['Year'] <= 2023)]
fig, ax1 = plt.subplots(figsize=(10, 6))

color_inf = '#d62728' # Piros az inflációnak
ax1.set_xlabel('Év', fontsize=12)
ax1.set_ylabel('Inflációs ráta (%)', color=color_inf, fontsize=12, weight='bold')
line1 = ax1.plot(df_stag['Year'], df_stag['Inflation_Rate'], color=color_inf,
                 linewidth=3, marker='o', markersize=8, label='Infláció (%)')
ax1.tick_params(axis='y', labelcolor=color_inf)
ax1.set_xticks([2021, 2022, 2023])

ax2 = ax1.twinx()
color_gdp = '#1f77b4' # Kék a GDP-nek
ax2.set_ylabel('Nominális GDP (USD)', color=color_gdp, fontsize=12, weight='bold')
line2 = ax2.plot(df_stag['Year'], df_stag['GDP_USD'], color=color_gdp,
                 linewidth=3, marker='s', markersize=8, label='GDP (USD)')
ax2.tick_params(axis='y', labelcolor=color_gdp)

lines = line1 + line2
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc='upper left')

plt.title('Stagflációs nyomás Magyarországon (2021 - 2023)', fontsize=14, pad=15)
fig.tight_layout()
plt.savefig(f'{output_dir}/stagflacio_dual_axis_hun_2021_2023.png', dpi=300)
plt.close()


# ---------------------------------------------------------
# 3. Y-TENGELY SKÁLÁK GLOBÁLIS EGYSÉGESÍTÉSE
# ---------------------------------------------------------
print("Y-tengely skálák egységesítése országonként és mutatónként...")
y_limits = {}
df_all_models = pd.concat([df_arima, df_arimax, df_var])

for c in countries_to_plot:
    y_limits[c] = {}
    for ind in indicators:
        # A globális minimum/maximum keresése az összes modell és a történelmi adatok között
        act_min = df_actual[(df_actual['Country'] == c) & (df_actual['Year'] >= 2010)][ind].min()
        act_max = df_actual[(df_actual['Country'] == c) & (df_actual['Year'] >= 2010)][ind].max()

        pred_min = df_all_models[df_all_models['Country'] == c][ind].min()
        pred_max = df_all_models[df_all_models['Country'] == c][ind].max()

        overall_min = min(act_min, pred_min) if pd.notna(act_min) and pd.notna(pred_min) else act_min
        overall_max = max(act_max, pred_max) if pd.notna(act_max) and pd.notna(pred_max) else act_max

        # 10% esztétikai margó (padding) alkalmazása a grafikon szélén
        padding = (overall_max - overall_min) * 0.1 if pd.notna(overall_max) else 0
        y_limits[c][ind] = (overall_min - padding, overall_max + padding)


# ---------------------------------------------------------
# 4. UNIVERZÁLIS FÜGGVÉNY A WHAT-IF MODELLEK GENERÁLÁSÁRA
# ---------------------------------------------------------
def generate_model_plots(df_model, model_base_name, extra_title=""):
    """Dinamikusan legenerálja a kimeneti grafikonokat almappákba rendezve."""
    model_orders = df_model['Model_Order'].unique()
    scenarios = df_model['Scenario'].unique()

    for order in model_orders:
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

                mutatok = [
                    ('GDP_USD', 'GDP (USD)'),
                    ('Inflation_Rate', 'Infláció (%)'),
                    ('Public_Debt_Pct', 'Államadósság (GDP %)'),
                    ('Budget_Deficit_Pct', 'Költségvetési egyenleg (GDP %)')
                ]

                # Minden indikátorról önálló grafikon készül
                for col_name, title in mutatok:
                    plt.figure(figsize=(10, 6))
                    ax = plt.gca()

                    plt.title(
                        f'{vizsgalt_orszag}: Tényadatok vs. {model_base_name}({formatted_order}) {extra_title}[{scenario.replace("_", " ")}]\n{title}',
                              fontsize=14, weight='bold', pad=15)

                    sns.lineplot(data=df_akt, x='Year', y=col_name, ax=ax, label='Tényadatok', color='#1f77b4',
                                 linewidth=2.5)
                    sns.lineplot(data=df_pred, x='Year', y=col_name, ax=ax, label=f'{model_base_name} trend',
                                 color='#ff7f0e', linestyle='--', linewidth=2.5)

                    ax.set_xlabel('Év')
                    ax.set_ylabel('')

                    # Az előre kalkulált globális Y-tengely rögzítése az összehasonlíthatóságért
                    if pd.notna(y_limits[vizsgalt_orszag][col_name][0]):
                        ax.set_ylim(y_limits[vizsgalt_orszag][col_name])

                    ax.axvline(toreSpont_ev, color='red', linestyle=':', alpha=0.7, label=f'{toreSpont_ev} (Töréspont)')
                    ax.legend(loc='best')

                    plt.tight_layout()
                    plt.savefig(f'{order_dir}/what_if_{vizsgalt_orszag}_{scenario}_{col_name}.png', dpi=300)
                    plt.close()


# ---------------------------------------------------------
# 5. FÜGGVÉNY HÍVÁSA MINDEGYIK MODELLTÍPUSRA
# ---------------------------------------------------------
print("\n--- ARIMA diagramok rajzolása ---")
generate_model_plots(df_arima, "ARIMA")

print("\n--- ARIMAX diagramok rajzolása ---")
generate_model_plots(df_arimax, "ARIMAX", extra_title="(Külső: Olajár) ")

print("\n--- VAR diagramok rajzolása ---")
generate_model_plots(df_var, "VAR")

print(f"\nMinden almappa és ábra sikeresen elkészült!")