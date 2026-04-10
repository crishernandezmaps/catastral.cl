import comunasData from './data/comunas.json'

export const API_URL = import.meta.env.VITE_API_URL || '/api'

// Derive tier pricing dynamically from comunas data
const baseComunas = comunasData.filter(c => c.tier !== 'BUNDLE')
const tierMap = {}
baseComunas.forEach(c => {
  if (!tierMap[c.tier]) {
    tierMap[c.tier] = c.precio
  }
})

const tierOrder = ['XL', 'L', 'M', 'S', 'XS']
export const TIER_PRICES = tierOrder
  .filter(t => tierMap[t] !== undefined)
  .map(tier => ({ tier, price: tierMap[tier] }))
