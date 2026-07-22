'use strict';

const DEFAULT_BASE_DECIMALS = 6;
const DEFAULT_QUOTE_DECIMALS = 9;

function toFiniteNumber(value) {
  if (value == null) return null;
  try {
    const number = Number(typeof value === 'object' && value.toString
      ? value.toString()
      : value);
    return Number.isFinite(number) ? number : null;
  } catch (_) {
    return null;
  }
}

function getVirtualQuoteReservesRaw(state) {
  const raw = toFiniteNumber(state?.pool?.virtualQuoteReserves);
  return raw != null && raw >= 0 ? raw : null;
}

function priceDetailsFromRawState(
  state,
  baseDecimals = DEFAULT_BASE_DECIMALS,
  quoteDecimals = DEFAULT_QUOTE_DECIMALS,
) {
  if (!state) return null;

  const baseRaw = toFiniteNumber(state.poolBaseAmount);
  const rawQuoteRaw = toFiniteNumber(state.poolQuoteAmount);
  const virtualQuoteRaw = getVirtualQuoteReservesRaw(state);
  const baseScale = 10 ** Number(baseDecimals);
  const quoteScale = 10 ** Number(quoteDecimals);

  if (
    baseRaw == null || baseRaw <= 0 ||
    rawQuoteRaw == null || rawQuoteRaw <= 0 ||
    virtualQuoteRaw == null ||
    !Number.isFinite(baseScale) || baseScale <= 0 ||
    !Number.isFinite(quoteScale) || quoteScale <= 0
  ) return null;

  const baseUi = baseRaw / baseScale;
  const rawQuoteUi = rawQuoteRaw / quoteScale;
  return priceDetailsFromUiReserves(baseUi, rawQuoteUi, state, quoteDecimals);
}

function priceDetailsFromUiReserves(
  baseUi,
  rawQuoteUi,
  state,
  quoteDecimals = DEFAULT_QUOTE_DECIMALS,
) {
  const base = Number(baseUi);
  const rawQuote = Number(rawQuoteUi);
  const virtualQuoteRaw = getVirtualQuoteReservesRaw(state);
  const quoteScale = 10 ** Number(quoteDecimals);

  if (
    !Number.isFinite(base) || base <= 0 ||
    !Number.isFinite(rawQuote) || rawQuote <= 0 ||
    virtualQuoteRaw == null ||
    !Number.isFinite(quoteScale) || quoteScale <= 0
  ) return null;

  const virtualQuoteUi = virtualQuoteRaw / quoteScale;
  const effectiveQuoteUi = rawQuote + virtualQuoteUi;
  return {
    rawPrice: rawQuote / base,
    effectivePrice: effectiveQuoteUi / base,
    rawQuoteUi: rawQuote,
    virtualQuoteUi,
    effectiveQuoteUi,
  };
}

function constantProductAfterBaseUi({
  baseBeforeUi,
  baseAfterUi,
  rawQuoteBeforeUi,
  state,
  quoteDecimals = DEFAULT_QUOTE_DECIMALS,
}) {
  const before = priceDetailsFromUiReserves(
    baseBeforeUi,
    rawQuoteBeforeUi,
    state,
    quoteDecimals,
  );
  const baseAfter = Number(baseAfterUi);
  if (!before || !Number.isFinite(baseAfter) || baseAfter <= 0) return null;

  const effectiveQuoteAfterUi = (
    before.effectiveQuoteUi * Number(baseBeforeUi)
  ) / baseAfter;
  const rawQuoteAfterUi = effectiveQuoteAfterUi - before.virtualQuoteUi;
  if (!Number.isFinite(rawQuoteAfterUi) || rawQuoteAfterUi <= 0) return null;

  return {
    priceBefore: before.effectivePrice,
    priceAfter: effectiveQuoteAfterUi / baseAfter,
    rawPriceBefore: before.rawPrice,
    rawPriceAfter: rawQuoteAfterUi / baseAfter,
    rawQuoteAfterUi,
    effectiveQuoteAfterUi,
    virtualQuoteUi: before.virtualQuoteUi,
    quoteAmountUi: Math.abs(before.effectiveQuoteUi - effectiveQuoteAfterUi),
  };
}

module.exports = {
  getVirtualQuoteReservesRaw,
  priceDetailsFromRawState,
  priceDetailsFromUiReserves,
  constantProductAfterBaseUi,
};
