import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.api import VAR

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# 1. GLOBÁLIS BEÁLLÍTÁSOK
# ---------------------------------------------------------
SCENARIOS = {
    '2020_Covid19': (2020, 2025),
    '2022_Ukr_Konfliktus': (2022, 2027)
}

# A vizsgált változók (a VAR modell egy zárt rendszerként, egyben kezeli őket)
indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct']

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
        country_data = df[df['Country'] == country].sort_values('Year')

        # Tanuló adathalmaz létrehozása a töréspont előtt
        train_df = country_data[country_data['Year'] < start_year][indicators]

        # A VAR modell szigorú adatigénye: sorok elvetése, ahol bármelyik indikátor hiányzik (NaN)
        train_df = train_df.dropna()

        pred_df = pd.DataFrame({
            'Model_Order': ['VAR_L4_AIC'] * steps,  # Dinamikus késleltetés AIC alapon, max 4 év
            'Scenario': [scenario_name] * steps,
            'Country': [country] * steps,
            'Year': forecast_years
        })

        if len(train_df) < 10:
            for ind in indicators:
                pred_df[ind] = np.nan
            all_forecasts.append(pred_df)
            continue

        try:
            # VAR modell inicializálása
            model = VAR(train_df)

            # Optimális késleltetés (lag) kiválasztása az Akaike Információs Kritérium (AIC) alapján
            model_fit = model.fit(maxlags=4, ic='aic')

            # Előrejelzés generálása: az algoritmus igényli az utolsó 'lag_order' történelmi adatot bázisként
            lag_order = model_fit.k_ar
            forecast_input = train_df.values[-lag_order:]
            forecast = model_fit.forecast(y=forecast_input, steps=steps)

            # Az eredmény mátrix (évek x mutatók) oszlopokra bontása
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

print(f"\nKész! A VAR előrejelzések sikeresen elmentve: {output_file}")