import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.arima.model import ARIMA

warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# 1. GLOBÁLIS BEÁLLÍTÁSOK ÉS MODELLEZÉSI PARAMÉTEREK
# ---------------------------------------------------------
SCENARIOS = {
    '2020_Covid19': (2020, 2025),
    '2022_Ukr_Konfliktus': (2022, 2027)
}

# (p, d, q) paraméterek: Autoregresszív(p), Integrált(d), Mozgóátlag(q) komponensek
ARIMA_ORDERS = [
    (1, 1, 1),  # Alapmodell
]

print("Adatok beolvasása (merged_data.csv)...")
df = pd.read_csv('../data/merged_data.csv')

indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct']
countries = df['Country'].unique()
all_forecasts = []

# ---------------------------------------------------------
# 2. ARIMA MODELLEZÉS (Iteráció: Paraméter -> Szcenárió -> Ország)
# ---------------------------------------------------------
for order in ARIMA_ORDERS:
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

            # Előrejelzési kimeneti keret inicializálása
            pred_df = pd.DataFrame({
                'Model_Order': [order_str] * steps,
                'Scenario': [scenario_name] * steps,
                'Country': [country] * steps,
                'Year': forecast_years
            })

            # Egyváltozós modellezés indikátoronként
            for ind in indicators:
                # Tanuló adathalmaz szűrése a vizsgált exogén sokk (töréspont) előtti időszakra
                train_data = country_data[country_data['Year'] < start_year][ind].dropna().values

                # Adathiány kezelése: minimum 10 évnyi történelmi adat szükséges a konvergenciához
                if len(train_data) < 10:
                    pred_df[ind] = np.nan
                    continue

                try:
                    # Modell betanítása és "what-if" előrejelzés generálása
                    model = ARIMA(train_data, order=order)
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

print(f"\nKész! Az összes paraméter előrejelzése elmentve: {output_file}")