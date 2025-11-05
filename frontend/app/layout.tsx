import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'Real Estate Zip Code Finder',
  description: 'Find investment opportunities by zip code',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  )
}

