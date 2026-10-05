import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.api import VAR

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# GLOBÁLIS BEÁLLÍTÁSOK
# ---------------------------------------------------------
SCENARIOS = {
    '2020_Covid19': (2020, 2025),
    '2022_Ukr_Konfliktus': (2022, 2027)
}

# A vizsgált változók (a VAR modell egyben kezeli őket)
indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA
# ---------------------------------------------------------
print("Adatok beolvasása (merged_data.csv)...")
df = pd.read_csv('../data/merged_data.csv')
countries = df['Country'].unique()
all_forecasts = []

# ---------------------------------------------------------
# 2. VAR MODELLEZÉS SZCENÁRIÓNKÉNT ÉS ORSZÁGONKÉNT
# ---------------------------------------------------------
for scenario_name, (start_year, end_year) in SCENARIOS.items():
    print(f"\n--- Szcenárió futtatása: {scenario_name} ({start_year}-{end_year}) ---")

    forecast_years = list(range(start_year, end_year + 1))
    steps = len(forecast_years)

    for country in countries:
        # 1. Adatok szűrése és időrendbe állítása
        country_data = df[df['Country'] == country].sort_values('Year')

        # 2. Tanuló adathalmaz létrehozása a töréspont előtt
        train_df = country_data[country_data['Year'] < start_year][indicators]

        # A VAR modellnél nagyon fontos, hogy ne legyen NaN érték egyik oszlopban sem
        # Ezért csak azokat az éveket tartjuk meg, ahol mind a 4 mutató rendelkezésre áll
        train_df = train_df.dropna()

        # Alap kimeneti struktúra az előrejelzésnek
        pred_df = pd.DataFrame({
            # 'Model_Order': ['VAR_L1'] * steps,  # L1 jelöli az 1-es késleltetést (lag)
            # 'Model_Order': ['VAR_L3_AIC'] * steps,  # L1 jelöli az 1-es késleltetést (lag)
            'Model_Order': ['VAR_L4_AIC'] * steps,  # L1 jelöli az 1-es késleltetést (lag)
            'Scenario': [scenario_name] * steps,
            'Country': [country] * steps,
            'Year': forecast_years
        })

        # Ha túl kevés a közös történelmi adat (pl. < 10 év), átugorjuk
        if len(train_df) < 10:
            for ind in indicators:
                pred_df[ind] = np.nan
            all_forecasts.append(pred_df)
            continue

        try:
            # 3. VAR modell inicializálása és illesztése
            model = VAR(train_df)

            # Éves makroadatoknál az 1-es vagy 2-es késleltetés (lag) a legjobb.
            # Itt maxlags=1-et használunk, ami azt jelenti, hogy az előző év adatai magyarázzák a következőt.
            # model_fit = model.fit(maxlags=1)
            # model_fit = model.fit(maxlags=3, ic='aic')
            model_fit = model.fit(maxlags=4, ic='aic')

            # 4. Előrejelzés
            # A VAR forecast függvényének szüksége van az utolsó 'lag' darab történelmi adatra bázisként
            lag_order = model_fit.k_ar
            forecast_input = train_df.values[-lag_order:]

            # A predikció egy mátrixot ad vissza (évek x mutatók)
            forecast = model_fit.forecast(y=forecast_input, steps=steps)

            # 5. Eredmények betöltése a DataFrame-be
            for i, ind in enumerate(indicators):
                pred_df[ind] = forecast[:, i]

        except Exception as e:
            print(f"  Hiba {country} esetén: {e}")
            for ind in indicators:
                pred_df[ind] = np.nan

        all_forecasts.append(pred_df)

# ---------------------------------------------------------
# 3. EREDMÉNYEK ÖSSZEFŰZÉSE ÉS MENTÉSE
# ---------------------------------------------------------
final_var_df = pd.concat(all_forecasts, ignore_index=True)
output_file = '../data/var_scenarios.csv'
final_var_df.to_csv(output_file, index=False)

print(f"\n✅ Kész! A VAR előrejelzések sikeresen elmentve: {output_file}")