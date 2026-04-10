export const DISCOUNT_TIERS = [
  { min: 20, pct: 0.15, label: '15% dcto (20+ comunas)' },
  { min: 10, pct: 0.10, label: '10% dcto (10+ comunas)' },
  { min: 3, pct: 0.05, label: '5% dcto (3+ comunas)' },
]

export function calculateCartTotal(items) {
  const subtotal = items.reduce((sum, item) => sum + (item.precio || 0), 0)
  const count = items.length

  let discount = null
  for (const tier of DISCOUNT_TIERS) {
    if (count >= tier.min) {
      discount = tier
      break
    }
  }

  const discountAmount = discount ? Math.floor(subtotal * discount.pct) : 0
  const total = subtotal - discountAmount

  return { subtotal, discount, discountAmount, total }
}
