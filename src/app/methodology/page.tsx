export default function Methodology() {
  return (
    <article className="prose dark:prose-invert">
      <h1>Methodology & limitations</h1>
      <ul>
        <li>We aggregate anonymised DVSA MOT tests (GB) since 2005 by make, model and first registration year to compute age-adjusted pass rates and mileage percentiles. Source: data.gov.uk.</li>
        <li>Failure categories map DVSA reason codes into simplified groups (brakes, suspension, tyres, emissions) for readability.</li>
        <li>Official CO₂/MPG figures come from VCA (WLTP/NEDC as provided). These are lab figures and may differ from real-world consumption.</li>
        <li>VED shown is indicative standard annual rate, derived from GOV.UK rate tables; first-year showroom rates and special cases are excluded.</li>
        <li>Interpretation: high failure rates can reflect maintenance history, tester behaviour, or usage profile; treat “red/amber/green” as a guide, not a verdict.</li>
      </ul>
      <h2>Attribution</h2>
      <p>Contains public sector information licensed under the <a href="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/">Open Government Licence v3.0</a>.</p>
      <h2>Sources</h2>
      <ul>
        <li><a href="https://www.data.gov.uk/dataset/c63fca52-ae4c-4b75-bab5-8b4735e1a4c9/anonymised-mot-tests-and-results">Anonymised MOT tests and results</a></li>
        <li><a href="https://www.gov.uk/government/statistical-data-sets/mot-testing-data-for-great-britain">DVSA MOT testing data & definitions</a></li>
        <li><a href="https://www.check-vehicle-recalls.service.gov.uk/">Vehicle recalls</a></li>
        <li><a href="https://www.vehicle-certification-agency.gov.uk/fuel-consumption-co2/">VCA CO₂/MPG</a></li>
        <li><a href="https://www.gov.uk/vehicle-tax-rate-tables">VED rate tables</a></li>
      </ul>
    </article>
  )
}
