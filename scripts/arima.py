import pandas as pd
import numpy as np
import warnings
from statsmodels.tsa.arima.model import ARIMA

# Figyelmeztetések (pl. konvergencia hibák) elrejtése a letisztultabb konzol kimenetért
warnings.filterwarnings("ignore")

# ---------------------------------------------------------
# GLOBÁLIS BEÁLLÍTÁSOK (Szcenáriók definiálása)
# ---------------------------------------------------------
# Formátum: 'Szcenárió neve': (Kezdő év, Utolsó előrejelzett év)
SCENARIOS = {
    '1991_Szovjetunio_Felbomlasa': (1991, 1996),
    '2008_Gazdasagi_Valsag': (2008, 2013),
    '2020_Covid19': (2020, 2025),
    '2022_Ukr_Konfliktus': (2022, 2027)
}

# ---------------------------------------------------------
# 1. ADATOK BEOLVASÁSA ÉS BEÁLLÍTÁSOK
# ---------------------------------------------------------
print("Adatok beolvasása (merged_data.csv)...")
df = pd.read_csv('../data/merged_data.csv')

# Változók és országok meghatározása
indicators = ['GDP_USD', 'Inflation_Rate', 'Public_Debt_Pct', 'Budget_Deficit_Pct']
countries = df['Country'].unique()

# Eredmények gyűjtésére szolgáló lista
all_forecasts = []

# ---------------------------------------------------------
# 2. ARIMA MODELLEZÉS SZCENÁRIÓNKÉNT, ORSZÁGONKÉNT, MUTATÓNKÉNT
# ---------------------------------------------------------
for scenario_name, (start_year, end_year) in SCENARIOS.items():
    print(f"\n--- Szcenárió futtatása: {scenario_name} ({start_year}-{end_year}) ---")

    forecast_years = list(range(start_year, end_year + 1))
    steps = len(forecast_years)

    for country in countries:
        # Kiszűrjük az adott ország adatait és időrendbe rakjuk
        country_data = df[df['Country'] == country].sort_values('Year')

        # Létrehozunk egy üres DataFrame-et az előrejelzéseknek (új 'Scenario' oszloppal)
        pred_df = pd.DataFrame({
            'Scenario': [scenario_name] * steps,
            'Country': [country] * steps,
            'Year': forecast_years
        })

        for ind in indicators:
            # A TANULÓ ADATHALMAZ: a töréspont előtti adatok
            train_data = country_data[country_data['Year'] < start_year][ind].dropna().values

            # Ha túl kevés a történelmi adat (pl. < 10 év), az ARIMA nem tud jól tanulni.
            # Ez például Észtország 1991-es adatainál fog aktiválódni, ahol nincsenek 1980 előtti szovjet adatok.
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
                # Ha a modell összedől, hagyjuk üresen
                pred_df[ind] = np.nan

        # Hozzáadjuk a kiszámolt periódust a nagy listához
        all_forecasts.append(pred_df)

# ---------------------------------------------------------
# 3. EREDMÉNYEK ÖSSZEFŰZÉSE ÉS MENTÉSE
# ---------------------------------------------------------
# Az összes ország és szcenárió adatát egyetlen nagy táblázattá fűzzük össze
final_arima_df = pd.concat(all_forecasts, ignore_index=True)

# Mentés új néven, hogy jelezzük a struktúraváltást
output_file = '../data/arima_scenarios.csv'
final_arima_df.to_csv(output_file, index=False)

print(f"\n✅ Kész! Az előrejelzések sikeresen elmentve: {output_file}")
print(f"Összesen {len(final_arima_df)} sor generálva.")