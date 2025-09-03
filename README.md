# Check-A-Car

**Check-A-Car** is an open-source project that transforms the UK’s publicly available **DVSA MOT dataset** into an accessible, visual resource for drivers, researchers, and enthusiasts.  

The goal: make it easy to explore reliability trends, common failure points, mileage patterns, emissions, and tax information across makes and models of cars tested in the UK.  

---

## Features

**Search by make and model** — see how different vehicles perform over time.  
**Aggregated MOT statistics** — pass rates, age curves, mileage distributions.  
**Failure reasons** — grouped into categories (brakes, tyres, emissions, etc.).  
**Recalls & defects** — timelines of official recalls by manufacturer and model.  
**Environmental data** — CO₂, MPG, and Vehicle Excise Duty (VED) bands.  
**Automated pipeline** — fresh datasets are pulled and processed weekly via GitHub Actions.  

---

## Data Sources

- [DVSA anonymised MOT results and failure items](https://www.data.gov.uk/dataset/c63fca52-ae4c-4b75-bab5-8b4735e1a4c9/anonymised-mot-tests-and-results)  
- [DVSA Recalls](https://www.gov.uk/check-vehicle-recall)  
- [VCA fuel consumption & CO₂ data](https://carfueldata.vehicle-certification-agency.gov.uk/)  
- [GOV.UK VED rates](https://www.gov.uk/vehicle-tax-rate-tables)  

All datasets are published under the **Open Government Licence (OGL v3.0)**.

---

## Repo Structure

etl/ # Python ETL pipeline (ingest, normalise, aggregate, publish)
data_raw/ # Raw downloaded MOT datasets
data_intermediate/ # Cleaned & Parquet intermediates
public/data/ # JSON outputs consumed by the website
.github/workflows/ # GitHub Actions workflows (ETL automation)


---

## Tech Stack

- **Python (pandas, pyarrow, parquet)** for ETL  
- **GitHub Actions** for CI/CD and weekly refreshes  
- **Next.js + React + Tailwind** for the website frontend  
- **GitHub Pages** for hosting  

---

## Status

🚧 **Work in progress.**  
Core ETL pipeline is live and publishing JSON datasets.  
Frontend visualisations are under active development.  

---

## Licence

This project and derived outputs are provided under the [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).
