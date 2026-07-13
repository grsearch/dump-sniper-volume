// v3.20: 波动率标签 — MutationObserver方式，不依赖主页面缓存
(function() {
  const L = 10, H = 15;
  const labels = { low: '🟢低波', mid: '🟡中波', high: '🔴高波' };
  const colors = { low: '#22c55e', mid: '#eab308', high: '#ef4444' };

  function tier(v) { return v == null || v < 0 ? 'unknown' : v < L ? 'low' : v >= H ? 'high' : 'mid'; }
  function badgeHTML(t, v) {
    if (!t || t === 'unknown') return '';
    const s = v != null ? v.toFixed(1)+'%' : '?';
    return ' <span style="font-size:10px;color:'+colors[t]+';background:'+colors[t]+'18;padding:1px 4px;border-radius:3px" title="pre_vol_5m='+s+'">'+labels[t]+'</span>';
  }

  // Cache API data
  let openData = [];
  let closedData = [];

  function fetchData() {
    fetch('/api/positions').then(r => r.json()).then(d => {
      openData = d.open || [];
      closedData = d.recent || [];
      injectOpen();
      injectClosed();
    }).catch(() => {});
  }

  function injectOpen() {
    const tbody = document.getElementById('openTbody');
    if (!tbody) return;
    const rows = tbody.querySelectorAll('tr');
    openData.forEach((pos, i) => {
      if (!rows[i]) return;
      const cell = rows[i].querySelectorAll('td')[1];
      if (!cell) return;
      if (cell.querySelector('[title*="pre_vol"]')) return;
      const t = tier(pos.preVol5m);
      if (t === 'unknown') return;
      cell.insertAdjacentHTML('beforeend', badgeHTML(t, pos.preVol5m));
    });
  }

  function injectClosed() {
    const tbody = document.getElementById('closedTbody');
    if (!tbody) return;
    const rows = tbody.querySelectorAll('tr');
    rows.forEach(row => {
      const cells = row.querySelectorAll('td');
      if (cells.length < 2) return;
      if (cells[1].querySelector('[title*="pre_vol"]')) return;
      const sym = cells[1]?.textContent?.trim()?.replace(/[🟢🟡🔴低高中波]/g, '').trim();
      const match = closedData.find(p => (p.symbol||'').trim() === sym);
      if (!match || match.pre_vol_5m_pct == null || match.pre_vol_5m_pct < 0) return;
      const t = tier(match.pre_vol_5m_pct);
      if (t === 'unknown') return;
      cells[1].insertAdjacentHTML('beforeend', badgeHTML(t, match.pre_vol_5m_pct));
    });
  }

  // Observe DOM mutations to re-inject when tables update
  const observer = new MutationObserver(() => {
    injectOpen();
    injectClosed();
  });

  function start() {
    const openTbody = document.getElementById('openTbody');
    const closedTbody = document.getElementById('closedTbody');
    if (openTbody) observer.observe(openTbody, { childList: true, subtree: true });
    if (closedTbody) observer.observe(closedTbody, { childList: true, subtree: true });
    fetchData();
    // Also refresh every 3s (sync with main refreshAll)
    setInterval(fetchData, 3500);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', start);
  } else {
    start();
  }
})();
