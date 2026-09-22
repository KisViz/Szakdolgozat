import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.arima.model import ARIMA

# Figyelmeztetések (pl. konvergencia hibák) elrejtése a letisztultabb konzol kimenetért
warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# GLOBÁLIS BEÁLLÍTÁSOK (Előrejelzés időtávja)
# ---------------------------------------------------------
# Ezeket a változókat átírva automatikusan változik a tanítás és az előrejelzés hossza
FORECAST_START_YEAR = 2019
FORECAST_END_YEAR = 2025

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS BEÁLLÍTÁSOK
# ---------------------------------------------------------
print("Adatok beolvasása (merged_data.csv)...")
df = pd.read_csv('../data/merged_data.csv')

# Változók és országok meghatározása
indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']
countries = df['Country'].unique()

# Az előrejelzés időtávja a globális változók alapján (a +1 kell, hogy az utolsó év is benne legyen)
forecast_years = list(range(FORECAST_START_YEAR, FORECAST_END_YEAR + 1))
steps = len(forecast_years)

# Eredmények gyűjtésére szolgáló lista
all_forecasts = []

print(f"\nARIMA modellek illesztése és előrejelzés folyamatban ({FORECAST_START_YEAR}-{FORECAST_END_YEAR})...")

# ---------------------------------------------------------
# 2. ARIMA MODELLEZÉS ORSZÁGONKÉNT ÉS MUTATÓNKÉNT
# ---------------------------------------------------------
for country in countries:
    print(f"Feldolgozás: {country}...")

    # Kiszűrjük az adott ország adatait és időrendbe rakjuk
    country_data = df[df['Country'] == country].sort_values('Year')

    # Létrehozunk egy üres DataFrame-et az előrejelzéseknek
    pred_df = pd.DataFrame({
        'Country': [country] * steps,
        'Year': forecast_years
    })

    for ind in indicators:
        # A TANULÓ ADATHALMAZ: a globális kezdőév előtti adatok, kihagyva a hiányzó értékeket (NaN)
        train_data = country_data[country_data['Year'] < FORECAST_START_YEAR][ind].dropna().values

        # Ha túl kevés a történelmi adat (pl. < 10 év), az ARIMA nem tud jól tanulni
        if len(train_data) < 10:
            pred_df[ind] = np.nan
            continue

        try:
            # ARIMA modell inicializálása és illesztése
            model = ARIMA(train_data, order=(1, 1, 1))
            model_fit = model.fit()

            # Előrejelzés a beállított lépésszámra
            forecast = model_fit.forecast(steps=steps)
            pred_df[ind] = forecast

        except Exception as e:
            # Ha a modell valamiért összedől (nagyon ritka), hagyjuk üresen
            print(f"  Hiba történt {ind} esetén: {e}")
            pred_df[ind] = np.nan

    # Hozzáadjuk az ország előrejelzéseit a nagy listához
    all_forecasts.append(pred_df)

# ---------------------------------------------------------
# 3. EREDMÉNYEK ÖSSZEFŰZÉSE ÉS MENTÉSE
# ---------------------------------------------------------
# Az összes ország adatát egyetlen nagy táblázattá fűzzük össze
final_arima_df = pd.concat(all_forecasts, ignore_index=True)

# Mentés
final_arima_df.to_csv('../data/arima.csv', index=False)

print("\n✅ Kész! Az előrejelzések sikeresen elmentve: ../data/arima.csv")
print("\nAz arima.csv első néhány sora:")
print(final_arima_df.head(10))