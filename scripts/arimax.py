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

ARIMA_ORDERS = [
    (1, 1, 1),
    (0, 1, 1),
    (1, 1, 0),
    (1, 2, 1)
]

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS ELŐKÉSZÍTÉSE
# ---------------------------------------------------------
print("Adatok beolvasása (merged_data.csv)...")
df = pd.read_csv('../data/merged_data.csv')

indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']
exog_var = 'Oil_Price_USD'
countries = df['Country'].unique()
all_forecasts = []

# Ellenőrizzük, hogy az olajár oszlop létezik-e és nem üres-e
if df[exog_var].isnull().all():
    print("FIGYELEM: Az olajár adatok hiányoznak a merged_data.csv-ből! Ellenőrizd a merge.py futását.")

all_years = list(range(1960, 2028))
oil_series = df[['Year', exog_var]].dropna().drop_duplicates().set_index('Year').reindex(all_years)

# Biztosítjuk, hogy az olajár tiszta szám (float) formátumú legyen
oil_series = oil_series[exog_var].astype(float).ffill().bfill()

# ---------------------------------------------------------
# 2. ARIMAX MODELLEZÉS
# ---------------------------------------------------------
for order in ARIMA_ORDERS:
    order_str = f"{order[0]}_{order[1]}_{order[2]}"
    print(f"\n=======================================================")
    print(f" MODELLEZÉS ARIMAX({order[0]}, {order[1]}, {order[2]}) PARAMÉTEREKKEL")
    print(f"=======================================================")

    for scenario_name, (start_year, end_year) in SCENARIOS.items():
        print(f"  --- Szcenárió: {scenario_name} ({start_year}-{end_year}) ---")
        forecast_years = list(range(start_year, end_year + 1))
        steps = len(forecast_years)

        # JAVÍTÁS: .reshape(-1, 1) -> 2D oszlopmátrixszá alakítja az adatot, mert a statsmodels ezt várja
        exog_forecast = oil_series.loc[forecast_years].values.reshape(-1, 1)

        for country in countries:
            country_data = df[df['Country'] == country].sort_values('Year')

            pred_df = pd.DataFrame({
                'Model_Order': [order_str] * steps,
                'Scenario': [scenario_name] * steps,
                'Country': [country] * steps,
                'Year': forecast_years
            })

            for ind in indicators:
                train_years = country_data[country_data['Year'] < start_year]['Year'].values
                endog_train = country_data[country_data['Year'] < start_year][ind].values
                exog_train = oil_series.loc[train_years].values

                valid_idx = ~np.isnan(endog_train) & ~np.isnan(exog_train)
                endog_train_clean = endog_train[valid_idx]

                # JAVÍTÁS: itt is .reshape(-1, 1)-et kap a tanuló adat
                exog_train_clean = exog_train[valid_idx].reshape(-1, 1)

                if len(endog_train_clean) < 10:
                    pred_df[ind] = np.nan
                    continue

                try:
                    # ARIMAX illesztése: átadjuk az exog paramétert is
                    model = ARIMA(endog_train_clean, exog=exog_train_clean, order=order)
                    model_fit = model.fit()

                    forecast = model_fit.forecast(steps=steps, exog=exog_forecast)
                    pred_df[ind] = np.array(forecast)

                except Exception as e:
                    # ÚJDONSÁG: Most már kiírja a konzolra, ha valamiért elszáll a matek!
                    print(f"    [!] Hiba a(z) {country} - {ind} számolásakor: {e}")
                    pred_df[ind] = np.nan

            all_forecasts.append(pred_df)

# ---------------------------------------------------------
# 3. EREDMÉNYEK ÖSSZEFŰZÉSE ÉS MENTÉSE
# ---------------------------------------------------------
final_arimax_df = pd.concat(all_forecasts, ignore_index=True)
output_file = '../data/arimax_scenarios.csv'
final_arimax_df.to_csv(output_file, index=False)

print(f"\n✅ Kész! Az ARIMAX előrejelzések sikeresen elmentve: {output_file}")