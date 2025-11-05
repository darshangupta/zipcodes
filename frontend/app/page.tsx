'use client'

import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Label } from '@/components/ui/label'
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table'

interface ZipData {
  zip: string
  city: string
  state: string
  score: number
  cap_rate: number
  cash_on_cash: number
  dscr: number
  cash_needed: number
  price: number
  rent: number
  eff_tax_rate?: number
  inventory_hits?: number
  crime_index?: number
}

interface MetaData {
  states_allowlist: string[]
  cap_threshold: number
  min_dscr: number
  max_cash: number
  loan: {
    rate: number
    term_years: number
    down_payment_pct: number
  }
  scoring_weights: Record<string, number>
}

export default function Home() {
  const [meta, setMeta] = useState<MetaData | null>(null)
  const [zips, setZips] = useState<ZipData[]>([])
  const [loading, setLoading] = useState(true)
  const [filters, setFilters] = useState({
    state: '',
    min_cap: '',
    max_cash: '',
    min_dscr: '',
    min_coc: '',
    limit: '100',
  })

  useEffect(() => {
    fetchMeta()
    fetchZips()
  }, [])

  const fetchMeta = async () => {
    try {
      const res = await fetch('http://localhost:8000/api/meta')
      const data = await res.json()
      setMeta(data)
      // Set default filters from meta
      setFilters(prev => ({
        ...prev,
        max_cash: data.max_cash?.toString() || '',
        min_dscr: data.min_dscr?.toString() || '',
      }))
    } catch (error) {
      console.error('Error fetching meta:', error)
    }
  }

  const fetchZips = async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams()
      if (filters.state) params.append('state', filters.state)
      if (filters.min_cap) params.append('min_cap', filters.min_cap)
      if (filters.max_cash) params.append('max_cash', filters.max_cash)
      if (filters.min_dscr) params.append('min_dscr', filters.min_dscr)
      if (filters.min_coc) params.append('min_coc', filters.min_coc)
      if (filters.limit) params.append('limit', filters.limit)

      const res = await fetch(`http://localhost:8000/api/zips?${params}`)
      const data = await res.json()
      setZips(data)
    } catch (error) {
      console.error('Error fetching zips:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleFilterChange = (key: string, value: string) => {
    setFilters(prev => ({ ...prev, [key]: value }))
  }

  const handleExport = () => {
    const params = new URLSearchParams()
    if (filters.state) params.append('state', filters.state)
    if (filters.min_cap) params.append('min_cap', filters.min_cap)
    if (filters.max_cash) params.append('max_cash', filters.max_cash)
    if (filters.min_dscr) params.append('min_dscr', filters.min_dscr)
    if (filters.min_coc) params.append('min_coc', filters.min_coc)

    window.open(`http://localhost:8000/api/export.csv?${params}`, '_blank')
  }

  const calculateBuyBox = () => {
    if (!meta) return { maxPrice: 0, formula: '' }
    const maxCash = parseFloat(filters.max_cash) || meta.max_cash
    const ltv = 1 - meta.loan.down_payment_pct
    const closing = 0.03 // from config
    const rehab = 0
    const reserves = 3 // months
    
    // Approximate: maxPrice = maxCash / ((1-ltv) + closing + rehab + reserves_factor)
    // Simplified calculation
    const downPct = meta.loan.down_payment_pct
    const totalPct = downPct + closing
    const maxPrice = maxCash / totalPct
    
    return {
      maxPrice: Math.round(maxPrice),
      formula: `Max Cash / (Down Payment % + Closing %)`
    }
  }

  const buyBox = calculateBuyBox()

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-7xl mx-auto">
        <h1 className="text-3xl font-bold mb-2 text-gray-900">Real Estate Zip Code Finder</h1>
        <p className="text-gray-600 mb-8">Find investment opportunities by zip code</p>

        {/* Filters */}
        <div className="bg-white p-6 rounded-lg shadow-sm border border-gray-200 mb-6">
          <h2 className="text-xl font-semibold mb-4 text-gray-900">Filters</h2>
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <div>
              <Label htmlFor="state" className="mb-2 block text-sm font-medium text-gray-700">State</Label>
              <Select value={filters.state || "all"} onValueChange={(value) => handleFilterChange('state', value === "all" ? "" : value)}>
                <SelectTrigger id="state" className="w-full">
                  <SelectValue placeholder="All States" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All States</SelectItem>
                  {meta?.states_allowlist.map(s => (
                    <SelectItem key={s} value={s}>{s}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <Label htmlFor="min_cap" className="mb-2 block text-sm font-medium text-gray-700">Min Cap Rate</Label>
              <Input
                id="min_cap"
                type="number"
                step="0.001"
                value={filters.min_cap}
                onChange={(e) => handleFilterChange('min_cap', e.target.value)}
                placeholder={meta?.cap_threshold?.toFixed(3) || "0.075"}
              />
            </div>
            <div>
              <Label htmlFor="max_cash" className="mb-2 block text-sm font-medium text-gray-700">Max Cash Needed ($)</Label>
              <Input
                id="max_cash"
                type="number"
                value={filters.max_cash}
                onChange={(e) => handleFilterChange('max_cash', e.target.value)}
                placeholder={meta?.max_cash?.toString() || "60000"}
              />
            </div>
            <div>
              <Label htmlFor="min_dscr" className="mb-2 block text-sm font-medium text-gray-700">Min DSCR</Label>
              <Input
                id="min_dscr"
                type="number"
                step="0.1"
                value={filters.min_dscr}
                onChange={(e) => handleFilterChange('min_dscr', e.target.value)}
                placeholder={meta?.min_dscr?.toFixed(1) || "1.2"}
              />
            </div>
            <div>
              <Label htmlFor="min_coc" className="mb-2 block text-sm font-medium text-gray-700">Min Cash-on-Cash (%)</Label>
              <Input
                id="min_coc"
                type="number"
                step="0.01"
                value={filters.min_coc}
                onChange={(e) => handleFilterChange('min_coc', e.target.value)}
                placeholder="0.00"
              />
            </div>
            <div>
              <Label htmlFor="limit" className="mb-2 block text-sm font-medium text-gray-700">Results Limit</Label>
              <Input
                id="limit"
                type="number"
                value={filters.limit}
                onChange={(e) => handleFilterChange('limit', e.target.value)}
                placeholder="100"
              />
            </div>
          </div>
          <div className="mt-6 flex gap-3">
            <Button onClick={fetchZips} variant="outline" className="px-6">Apply Filters</Button>
            <Button onClick={handleExport} variant="outline" className="px-6">Export CSV</Button>
          </div>
        </div>

        {/* Buy Box Summary */}
        <div className="bg-gradient-to-r from-blue-50 to-indigo-50 p-6 rounded-lg border border-blue-100 mb-6">
          <h3 className="font-semibold mb-4 text-gray-900 text-lg">Buy-Box Summary</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-white rounded-md p-4 border border-gray-200">
              <p className="text-sm text-gray-600 mb-1">Max Cash Available</p>
              <p className="text-2xl font-bold text-gray-900">${parseFloat(filters.max_cash || meta?.max_cash?.toString() || '0').toLocaleString()}</p>
            </div>
            <div className="bg-white rounded-md p-4 border border-gray-200">
              <p className="text-sm text-gray-600 mb-1">Approx Price Cap</p>
              <p className="text-2xl font-bold text-blue-600">${buyBox.maxPrice.toLocaleString()}</p>
            </div>
            <div className="bg-white rounded-md p-4 border border-gray-200">
              <p className="text-sm text-gray-600 mb-1">Formula</p>
              <p className="text-sm font-medium text-gray-700">{buyBox.formula}</p>
            </div>
          </div>
        </div>

        {/* Results Table */}
        <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-xl font-semibold text-gray-900">
              Results {zips.length > 0 && <span className="text-sm font-normal text-gray-500">({zips.length} found)</span>}
            </h2>
          </div>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Score</TableHead>
                <TableHead>ZIP</TableHead>
                <TableHead>City</TableHead>
                <TableHead>State</TableHead>
                <TableHead>Cap Rate</TableHead>
                <TableHead>CoC</TableHead>
                <TableHead>DSCR</TableHead>
                <TableHead>Cash Needed</TableHead>
                <TableHead>Price</TableHead>
                <TableHead>Rent</TableHead>
                <TableHead>Tax</TableHead>
                <TableHead>Inventory</TableHead>
                <TableHead>Crime Idx</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={13} className="text-center py-8 text-slate-500">
                    Loading...
                  </TableCell>
                </TableRow>
              ) : zips.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={13} className="text-center py-8 text-slate-500">
                    No results found. Run the CLI first to generate data.
                  </TableCell>
                </TableRow>
              ) : (
                zips.map((zip) => (
                  <TableRow key={zip.zip}>
                    <TableCell>{zip.score.toFixed(3)}</TableCell>
                    <TableCell className="font-mono">{zip.zip}</TableCell>
                    <TableCell>{zip.city}</TableCell>
                    <TableCell>{zip.state}</TableCell>
                    <TableCell>{(zip.cap_rate * 100).toFixed(2)}%</TableCell>
                    <TableCell>{(zip.cash_on_cash * 100).toFixed(2)}%</TableCell>
                    <TableCell>{zip.dscr.toFixed(2)}</TableCell>
                    <TableCell>${zip.cash_needed.toLocaleString()}</TableCell>
                    <TableCell>${zip.price.toLocaleString()}</TableCell>
                    <TableCell>${zip.rent.toLocaleString()}</TableCell>
                    <TableCell>{zip.eff_tax_rate ? (zip.eff_tax_rate * 100).toFixed(2) + '%' : 'N/A'}</TableCell>
                    <TableCell>{zip.inventory_hits || 0}</TableCell>
                    <TableCell>{zip.crime_index?.toFixed(2) || '1.00'}</TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  )
}

