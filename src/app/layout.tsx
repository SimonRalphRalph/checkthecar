import './globals.css'
import Link from 'next/link'
import { Metadata } from 'next'

export const metadata: Metadata = {
  title: 'CheckTheCar – MOT reliability & running-costs',
  description: 'Contextual MOT reliability, recalls, CO₂/MPG & VED by model-year.',
  metadataBase: new URL('https://checkthecar.example')
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="h-full">
      <body className="min-h-full bg-white text-gray-900 dark:bg-zinc-950 dark:text-zinc-50">
        <header className="border-b">
          <div className="mx-auto max-w-6xl px-4 py-3 flex items-center justify-between">
            <Link href="/" className="font-bold">CheckTheCar</Link>
            <nav className="flex gap-4 text-sm">
              <Link href="/compare">Compare</Link>
              <Link href="/plate">Plate</Link>
              <Link href="/methodology">Methodology</Link>
              <button aria-label="Toggle high contrast" onClick={()=>{
                document.documentElement.classList.toggle('dark')
              }} className="rounded px-2 py-1 border">Contrast</button>
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-6xl px-4 py-6">{children}</main>
        <footer className="border-t text-xs text-gray-600 dark:text-gray-300">
          <div className="mx-auto max-w-6xl px-4 py-6 space-y-2">
            <p>Contains public sector information licensed under the <a className="underline" href="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/">Open Government Licence v3.0</a>.</p>
            <p>Data sources: DVSA MOT tests and results; DVSA recalls; VCA fuel/CO₂; GOV.UK VED tables.</p>
            <p>Informational only — always obtain a professional inspection.</p>
          </div>
        </footer>
      </body>
    </html>
  )
}
