import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.arima.model import ARIMA

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# GLOBÁLIS BEÁLLÍTÁSOK (Csak a kért 2 szcenárió)
# ---------------------------------------------------------
SCENARIOS = {
    '2020_Covid19': (2020, 2025),
    '2022_Ukr_Konfliktus': (2022, 2027)
}

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS BEÁLLÍTÁSOK
# ---------------------------------------------------------
print("Adatok beolvasása (merged_data.csv)...")
df = pd.read_csv('../data/merged_data.csv')

indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']
countries = df['Country'].unique()
all_forecasts = []

# ---------------------------------------------------------
# 2. ARIMA MODELLEZÉS SZCENÁRIÓNKÉNT, ORSZÁGONKÉNT, MUTATÓNKÉNT
# ---------------------------------------------------------
for scenario_name, (start_year, end_year) in SCENARIOS.items():
    print(f"\n--- Szcenárió futtatása: {scenario_name} ({start_year}-{end_year}) ---")

    forecast_years = list(range(start_year, end_year + 1))
    steps = len(forecast_years)

    for country in countries:
        country_data = df[df['Country'] == country].sort_values('Year')

        pred_df = pd.DataFrame({
            'Scenario': [scenario_name] * steps,
            'Country': [country] * steps,
            'Year': forecast_years
        })

        for ind in indicators:
            train_data = country_data[country_data['Year'] < start_year][ind].dropna().values

            if len(train_data) < 10:
                pred_df[ind] = np.nan
                continue

            try:
                model = ARIMA(train_data, order=(1, 1, 1))
                model_fit = model.fit()
                pred_df[ind] = model_fit.forecast(steps=steps)
            except Exception:
                pred_df[ind] = np.nan

        all_forecasts.append(pred_df)

# ---------------------------------------------------------
# 3. EREDMÉNYEK ÖSSZEFŰZÉSE ÉS MENTÉSE
# ---------------------------------------------------------
final_arima_df = pd.concat(all_forecasts, ignore_index=True)
output_file = '../data/arima_scenarios.csv'
final_arima_df.to_csv(output_file, index=False)

print(f"\n✅ Kész! Az előrejelzések sikeresen elmentve: {output_file}")