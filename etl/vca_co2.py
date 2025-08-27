# etl/vca_co2.py
import pandas as pd, requests
from io import BytesIO

# CSV access info reference: VCA states current and archive CSVs are downloadable.  [oai_citation:16‡Vehicle Certification Agency](https://www.vehicle-certification-agency.gov.uk/fuel-consumption-co2/fuel-consumption-and-co2-tools-accessibility/?utm_source=chatgpt.com)

def load_vca_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(BytesIO(r.content))
    # Assume columns: Manufacturer, Model, Variant, Fuel, CO2, MPG(Combined), Test (WLTP/NEDC)
    df.rename(columns=lambda c: c.strip().lower().replace(" ", "_"), inplace=True)
    df["make"] = df["manufacturer"].str.title().str.strip()
    df["model_family"] = df["model"].str.extract(r"^([A-Za-z0-9 ]+)", expand=False).str.strip().str.title()
    df["test_cycle"] = df.get("test", "NEDC").str.upper()
    return df[["make","model_family","fuel","co2","mpg_combined","test_cycle"]]
