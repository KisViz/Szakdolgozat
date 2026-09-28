import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.arima.model import ARIMA

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# GLOBÁLIS BEÁLLÍTÁSOK
# ---------------------------------------------------------
SCENARIOS = {
    '2020_Covid19': (2020, 2025),
    '2022_Ukr_Konfliktus': (2022, 2027)
}

# Az általunk vizsgált ARIMA (p, d, q) paraméterek listája
ARIMA_ORDERS = [
    (1, 1, 1),  # Alapmodell
    (0, 1, 1),  # Mozgóátlag hangsúlyos
    (1, 1, 0),  # Autoregresszív hangsúlyos
    (1, 2, 1)   # Kétszeres differenciálás (erős trendhez)
]

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA
# ---------------------------------------------------------
print("Adatok beolvasása (merged_data.csv)...")
df = pd.read_csv('../data/merged_data.csv')

indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']
countries = df['Country'].unique()
all_forecasts = []

# ---------------------------------------------------------
# 2. ARIMA MODELLEZÉS (Paraméter -> Szcenárió -> Ország)
# ---------------------------------------------------------
for order in ARIMA_ORDERS:
    # Sztringgé alakítjuk a mappa és fájlnevekhez (pl. "1_1_1")
    order_str = f"{order[0]}_{order[1]}_{order[2]}"
    print(f"\n=======================================================")
    print(f" MODELLEZÉS ARIMA({order[0]}, {order[1]}, {order[2]}) PARAMÉTEREKKEL")
    print(f"=======================================================")

    for scenario_name, (start_year, end_year) in SCENARIOS.items():
        print(f"  --- Szcenárió: {scenario_name} ({start_year}-{end_year}) ---")
        forecast_years = list(range(start_year, end_year + 1))
        steps = len(forecast_years)

        for country in countries:
            country_data = df[df['Country'] == country].sort_values('Year')

            # Új oszlop: Model_Order
            pred_df = pd.DataFrame({
                'Model_Order': [order_str] * steps,
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
                    # A statikus (1,1,1) helyett a ciklus aktuális 'order' változóját kapja
                    model = ARIMA(train_data, order=order)
                    model_fit = model.fit()
                    pred_df[ind] = model_fit.forecast(steps=steps)
                except Exception:
                    pred_df[ind] = np.nan

            all_forecasts.append(pred_df)

# ---------------------------------------------------------
# 3. MENTÉS
# ---------------------------------------------------------
final_arima_df = pd.concat(all_forecasts, ignore_index=True)
output_file = '../data/arima_scenarios.csv'
final_arima_df.to_csv(output_file, index=False)

print(f"\n✅ Kész! Az összes paraméter előrejelzése elmentve: {output_file}")