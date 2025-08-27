# etl/ved.py
"""
Computes indicative annual VED based on first registration date and CO2 band.
Backed by GOV.UK tables & V149 PDF (April 2025). We simplify:
 - Cars registered 1 Mar 2001–31 Mar 2017: CO2 bands A–M
 - Cars from 1 Apr 2017: standard rate + expensive car supplement (if applicable)
 - We expose "band_label" and "annual_standard_rate" (ignore first-year showroom rate)
References: GOV.UK rate tables + V149 PDFs.   [oai_citation:17‡GOV.UK](https://www.gov.uk/vehicle-tax-rate-tables?utm_source=chatgpt.com) [oai_citation:18‡GOV.UK](https://assets.publishing.service.gov.uk/media/67d1b940a6d78876a3fb0a67/v149-rates-of-vehicle-tax-for-cars-motorcycles-light-goods-vehicles-and-private-light-goods-vehicles-april-2025.pdf?utm_source=chatgpt.com)
"""
from dataclasses import dataclass

@dataclass
class Ved:
    band_label: str
    annual_gbp: int
    notes: str

def compute_ved(first_reg_year: int, co2_g_km: int, list_price_gt_40k: bool=False) -> Ved:
    if 2001 <= first_reg_year <= 2016:
        # Simplified bands (example numbers — check PDF in CI; keep JSON mapping in repo for accuracy)
        bands = [
            (100, "A", 0), (110, "B", 20), (120, "C", 35), (130, "D", 150),
            (140, "E", 180), (150, "F", 200), (165, "G", 240), (175, "H", 290),
            (185, "I", 320), (200, "J", 365), (225, "K", 395), (255, "L", 675),
            (10_000, "M", 695),
        ]
        for limit, label, amt in bands:
            if co2_g_km <= limit:
                return Ved(label, amt, "Pre-2017 CO₂ banding (standard rate).")
    elif first_reg_year >= 2017:
        base = 195  # Standard rate 2025/26 example; confirm yearly in etl from V149.  [oai_citation:19‡GOV.UK](https://assets.publishing.service.gov.uk/media/67d1b940a6d78876a3fb0a67/v149-rates-of-vehicle-tax-for-cars-motorcycles-light-goods-vehicles-and-private-light-goods-vehicles-april-2025.pdf?utm_source=chatgpt.com)
        supplement = 425 if list_price_gt_40k else 0  # 5-year supplement
        return Ved("Standard", base + supplement, "Post-2017 standard rate; first-year differs.")
    return Ved("Unknown", 0, "Unable to compute; missing inputs.")
