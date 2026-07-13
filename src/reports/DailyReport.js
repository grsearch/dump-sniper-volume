'use strict';

const fs = require('fs');
const path = require('path');
const cron = require('node-cron');
const { config } = require('../config');
const { bjtDayRange, bjtDateString, bjtIsoString } = require('../utils/bjt');

class DailyReport {
  /**
   * @param {Object} deps
   * @param {Object} deps.tradeLogger
   * @param {Object} deps.tokenRegistry
   * @param {Object} [deps.competitorTracker] v3.17.32: 用于竞对分析
   */
  constructor({ tradeLogger, tokenRegistry, competitorTracker }) {
    this.tradeLogger = tradeLogger;
    this.tokenRegistry = tokenRegistry;
    this.competitorTracker = competitorTracker || null;
    this.db = tokenRegistry.db; // 共享 DB 连接
    fs.mkdirSync(path.resolve(config.storage.reportsDir), { recursive: true });
  }

  start() {
    cron.schedule(
      '0 0 * * *',
      () => {
        const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
        this.generateForDate(yesterday).catch((err) =>
          console.error(`[DailyReport] generation failed: ${err.message}`),
        );
      },
      { timezone: 'UTC' },
    );
    console.log('[DailyReport] scheduled: BJT 08:00 (UTC 00:00) daily');

    // v3.17.32: 启动时检查昨天报告是否存在，不存在则补生成
    // 防止重启错过 08:00 触发点
    this._backfillIfNeeded().catch((err) =>
      console.error(`[DailyReport] backfill failed: ${err.message}`),
    );
  }

  async _backfillIfNeeded() {
    const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000);
    const range = bjtDayRange(yesterday);
    const dateStr = range.dateStr;
    const filepath = path.join(path.resolve(config.storage.reportsDir), `${dateStr}.md`);
    if (!fs.existsSync(filepath)) {
      console.log(`[DailyReport] backfill: ${dateStr} report missing, generating...`);
      await this.generateForDate(yesterday);
    }
  }

  async generateForDate(date = new Date(Date.now() - 24 * 60 * 60 * 1000)) {
    const range = bjtDayRange(date);
    const dateStr = range.dateStr;

    const signals = this.tradeLogger.getSignalsInRange(range.startMs, range.endMs);
    const trades = this.tradeLogger.getTradesInRange(range.startMs, range.endMs);
    const positions = this.tradeLogger.getPositionsInRange(range.startMs, range.endMs);

    // v3.17.32: 深度数据
    const competitorPaired = this._queryCompetitorPaired(range.startMs, range.endMs);
    const headToHead = this._queryHeadToHead(range.startMs, range.endMs);
    const competitorTop = this._queryCompetitorTop(range.startMs, range.endMs);
    const exitReasonStats = this._queryExitReasonStats(range.startMs, range.endMs);
    const postExitAnalysis = this._queryPostExitAnalysis(range.startMs, range.endMs);

    const md = this._renderMarkdown({
      dateStr, signals, trades, positions,
      competitorPaired, headToHead, competitorTop,
      exitReasonStats, postExitAnalysis,
    });
    const filepath = path.join(path.resolve(config.storage.reportsDir), `${dateStr}.md`);
    fs.writeFileSync(filepath, md, 'utf-8');
    console.log(`[DailyReport] generated: ${filepath}`);
    return filepath;
  }

  // ============ v3.17.32 新增查询 ============

  /** 竞对在范围内的所有已配对 SELL */
  _queryCompetitorPaired(startMs, endMs) {
    try {
      return this.db.prepare(`
        SELECT
          cs.wallet, ca.label,
          cs.mint, cs.symbol,
          cb.ts AS buy_ts, cs.ts AS sell_ts,
          cb.price AS buy_price, cs.price AS sell_price,
          cb.sol_amount AS buy_sol, cs.sol_amount AS sell_sol,
          cs.pnl_sol, cs.pnl_pct, cs.hold_ms,
          cb.trigger_max_sell_sol, cb.trigger_max_impact_pct,
          cb.pool_quote_sol, cb.dump_to_buy_slot,
          cb.fdv_usd
        FROM competitor_trades cs
        JOIN competitor_trades cb ON cs.matched_buy_id = cb.id
        LEFT JOIN competitor_addresses ca ON cs.wallet = ca.address
        WHERE cs.side = 'SELL' AND cs.matched_buy_id IS NOT NULL
          AND cs.ts BETWEEN ? AND ?
      `).all(startMs, endMs);
    } catch (_) { return []; }
  }

  /** 同一 mint 上"我 vs 竞对"对决 */
  _queryHeadToHead(startMs, endMs) {
    try {
      return this.db.prepare(`
        SELECT
          p.position_id, p.mint, p.symbol,
          p.pnl_pct AS my_pnl_pct, p.exit_reason AS my_exit_reason,
          p.entry_price AS my_entry, p.exit_price AS my_exit,
          (SELECT AVG(ct.pnl_pct) FROM competitor_trades ct
           WHERE ct.mint = p.mint AND ct.side='SELL' AND ct.matched_buy_id IS NOT NULL
             AND ct.ts BETWEEN p.opened_at - 600000 AND p.opened_at + 1800000
          ) AS comp_avg_pnl_pct,
          (SELECT COUNT(DISTINCT ct.wallet) FROM competitor_trades ct
           WHERE ct.mint = p.mint AND ct.side='SELL' AND ct.matched_buy_id IS NOT NULL
             AND ct.ts BETWEEN p.opened_at - 600000 AND p.opened_at + 1800000
          ) AS comp_count,
          (SELECT AVG(ct.hold_ms / 1000.0) FROM competitor_trades ct
           WHERE ct.mint = p.mint AND ct.side='SELL' AND ct.matched_buy_id IS NOT NULL
             AND ct.ts BETWEEN p.opened_at - 600000 AND p.opened_at + 1800000
          ) AS comp_avg_hold_sec
        FROM positions p
        WHERE p.closed_at IS NOT NULL
          AND p.closed_at BETWEEN ? AND ?
        ORDER BY (COALESCE(comp_avg_pnl_pct,0) - p.pnl_pct) DESC
      `).all(startMs, endMs).filter(r => r.comp_count > 0);
    } catch (_) { return []; }
  }

  /** 当日竞对钱包排名 */
  _queryCompetitorTop(startMs, endMs) {
    try {
      return this.db.prepare(`
        SELECT
          COALESCE(ca.label, substr(cs.wallet, 1, 8)) AS competitor,
          cs.wallet,
          COUNT(*) AS n,
          ROUND(SUM(cs.pnl_sol), 3) AS total_pnl_sol,
          ROUND(AVG(cs.pnl_pct), 1) AS avg_pnl_pct,
          ROUND(SUM(CASE WHEN cs.pnl_sol > 0 THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate,
          ROUND(AVG(cs.hold_ms / 1000.0), 0) AS avg_hold_sec
        FROM competitor_trades cs
        LEFT JOIN competitor_addresses ca ON cs.wallet = ca.address
        WHERE cs.side = 'SELL' AND cs.matched_buy_id IS NOT NULL
          AND cs.ts BETWEEN ? AND ?
        GROUP BY cs.wallet
        HAVING n >= 2
        ORDER BY total_pnl_sol DESC
        LIMIT 10
      `).all(startMs, endMs);
    } catch (_) { return []; }
  }

  /** 我的 exit_reason 表现 */
  _queryExitReasonStats(startMs, endMs) {
    try {
      return this.db.prepare(`
        SELECT
          exit_reason,
          COUNT(*) AS n,
          ROUND(SUM(pnl_sol), 3) AS total_pnl,
          ROUND(AVG(pnl_pct), 1) AS avg_pnl_pct,
          ROUND(AVG(peak_pnl_pct), 1) AS avg_peak_pct,
          ROUND(AVG(time_to_peak_ms / 1000.0), 1) AS avg_time_to_peak_sec,
          ROUND(AVG((closed_at - opened_at) / 1000.0), 1) AS avg_hold_sec
        FROM positions
        WHERE closed_at BETWEEN ? AND ?
        GROUP BY exit_reason
        ORDER BY total_pnl DESC
      `).all(startMs, endMs);
    } catch (_) { return []; }
  }

  /** post_exit_stats 分析(如果 v3.17.31 已上) */
  _queryPostExitAnalysis(startMs, endMs) {
    try {
      const exists = this.db.prepare(
        `SELECT name FROM sqlite_master WHERE type='table' AND name='post_exit_stats'`
      ).get();
      if (!exists) return null;
      return this.db.prepare(`
        SELECT
          p.exit_reason,
          COUNT(*) AS n,
          ROUND(AVG(pe.max_pump_pct), 1) AS avg_post_pump,
          ROUND(AVG(pe.max_dump_pct), 1) AS avg_post_dump,
          ROUND(SUM(CASE WHEN pe.max_pump_pct > 10 THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 1) AS pct_sold_too_early
        FROM positions p
        JOIN post_exit_stats pe ON pe.position_id = p.position_id
        WHERE p.closed_at BETWEEN ? AND ?
        GROUP BY p.exit_reason
        ORDER BY n DESC
      `).all(startMs, endMs);
    } catch (_) { return null; }
  }

  // ============ 渲染 ============

  _renderMarkdown({ dateStr, signals, trades, positions,
    competitorPaired, headToHead, competitorTop,
    exitReasonStats, postExitAnalysis }) {
    const closed = positions.filter((p) => p.closed_at);
    const winners = closed.filter((p) => (p.pnl_sol ?? 0) > 0);
    const losers = closed.filter((p) => (p.pnl_sol ?? 0) <= 0);
    const totalPnl = closed.reduce((s, p) => s + (p.pnl_sol || 0), 0);

    const acceptedSignals = signals.filter((s) => s.accepted);
    const rejectedSignals = signals.filter((s) => !s.accepted);

    const rejectReasons = {};
    rejectedSignals.forEach((s) => {
      const r = (s.reject_reason || 'unknown').split(':')[0].trim(); // 截 prefix,合并同类
      rejectReasons[r] = (rejectReasons[r] || 0) + 1;
    });

    let md = `# Dump Sniper 每日报告 — ${dateStr} (BJT)\n\n`;
    md += `> 生成时间: ${bjtIsoString()} BJT\n`;
    md += `> 时间范围: ${dateStr} 00:00 ~ 24:00 (BJT)\n\n`;

    // ============ 总览 ============
    md += `## 📊 总览\n\n`;
    md += `| 指标 | 数值 |\n|---|---|\n`;
    md += `| 砸盘信号 | ${signals.filter((s) => s.kind === 'DUMP_DETECTED' || s.kind === 'BUY_SIGNAL').length} |\n`;
    md += `| 通过过滤的买入信号 | ${acceptedSignals.length} |\n`;
    md += `| 被拒信号 | ${rejectedSignals.length} (${signals.length ? ((rejectedSignals.length/signals.length)*100).toFixed(1) : 0}%) |\n`;
    md += `| 实际开仓 | ${positions.length} |\n`;
    md += `| 已平仓 | ${closed.length} |\n`;
    md += `| 盈利 / 亏损 | ${winners.length} / ${losers.length} |\n`;
    md += `| 胜率 | ${closed.length ? ((winners.length / closed.length) * 100).toFixed(1) : '-'}% |\n`;
    md += `| **总 PnL (SOL)** | **${totalPnl.toFixed(4)}** |\n`;
    md += `| 平均盈利 (SOL) | ${winners.length ? (winners.reduce((s,p)=>s+p.pnl_sol,0)/winners.length).toFixed(4) : '-'} |\n`;
    md += `| 平均亏损 (SOL) | ${losers.length ? (losers.reduce((s,p)=>s+p.pnl_sol,0)/losers.length).toFixed(4) : '-'} |\n\n`;

    // ============ exit_reason 表现 ============
    if (exitReasonStats && exitReasonStats.length > 0) {
      md += `## 🎯 退出路径表现\n\n`;
      md += `| 退出原因 | 笔数 | 总PnL | 平均PnL% | 平均峰值% | 到峰时间(s) | 平均持仓(s) |\n`;
      md += `|---|---|---|---|---|---|---|\n`;
      for (const r of exitReasonStats) {
        md += `| ${r.exit_reason || '-'} | ${r.n} | ${r.total_pnl >= 0 ? '+' : ''}${r.total_pnl} | ${r.avg_pnl_pct}% | ${r.avg_peak_pct || '-'}% | ${r.avg_time_to_peak_sec || '-'} | ${r.avg_hold_sec} |\n`;
      }
      md += `\n`;
    }

    // ============ 拒绝原因分布 ============
    if (Object.keys(rejectReasons).length) {
      md += `## ⏭ 拒绝原因 TOP\n\n`;
      md += `| 原因 | 次数 | 占比 |\n|---|---|---|\n`;
      const total = rejectedSignals.length;
      for (const [r, n] of Object.entries(rejectReasons).sort((a, b) => b[1] - a[1]).slice(0, 15)) {
        md += `| ${r} | ${n} | ${((n/total)*100).toFixed(1)}% |\n`;
      }
      md += `\n`;
    }

    // ============ 竞争对手对比 ============
    if (competitorTop && competitorTop.length > 0) {
      md += `## 🏆 竞争对手 Top 10\n\n`;
      md += `| 钱包 | 笔数 | 总PnL | 平均PnL% | 胜率 | 平均持仓(s) |\n`;
      md += `|---|---|---|---|---|---|\n`;
      for (const c of competitorTop) {
        md += `| ${c.competitor} | ${c.n} | ${c.total_pnl_sol >= 0 ? '+' : ''}${c.total_pnl_sol} | ${c.avg_pnl_pct}% | ${c.win_rate}% | ${c.avg_hold_sec} |\n`;
      }
      md += `\n`;
      // 跟我对比
      const myWinRate = closed.length ? (winners.length / closed.length) * 100 : 0;
      const myAvgHold = closed.length ? closed.reduce((s,p)=>s+(p.closed_at-p.opened_at),0)/closed.length/1000 : 0;
      md += `**我**: ${closed.length} 笔, 总PnL ${totalPnl.toFixed(4)} SOL, 胜率 ${myWinRate.toFixed(1)}%, 平均持仓 ${myAvgHold.toFixed(0)}s\n\n`;
    }

    if (headToHead && headToHead.length > 0) {
      md += `## ⚔️ 同 Mint 对决(我 vs 竞对)\n\n`;
      md += `*只列出当日有竞对同时交易的 mint。差距正数 = 竞对赚得多。*\n\n`;
      md += `| 代币 | 我 PnL% | 竞对均 PnL% | 差距 | 退出原因 | 竞对数 | 竞对平均持仓(s) |\n`;
      md += `|---|---|---|---|---|---|---|\n`;
      for (const r of headToHead.slice(0, 20)) {
        const diff = (r.comp_avg_pnl_pct || 0) - r.my_pnl_pct;
        md += `| ${r.symbol || r.mint.slice(0,6)} | ${r.my_pnl_pct?.toFixed(1)}% | ${r.comp_avg_pnl_pct?.toFixed(1)}% | ${diff > 0 ? '+' : ''}${diff.toFixed(1)}% | ${r.my_exit_reason} | ${r.comp_count} | ${r.comp_avg_hold_sec?.toFixed(0) || '-'} |\n`;
      }
      md += `\n`;
    }

    // 竞对赚钱单 vs 亏钱单的特征对比
    if (competitorPaired && competitorPaired.length >= 5) {
      const wins = competitorPaired.filter(c => c.pnl_pct > 0);
      const losses = competitorPaired.filter(c => c.pnl_pct < 0);
      if (wins.length > 0 && losses.length > 0) {
        const avg = (arr, field) => arr.length ? arr.reduce((s,x)=>s+(x[field]||0),0)/arr.length : 0;
        md += `## 🧬 竞对赚 vs 亏的特征对比\n\n`;
        md += `| 特征 | 赚钱单 (n=${wins.length}) | 亏钱单 (n=${losses.length}) |\n`;
        md += `|---|---|---|\n`;
        md += `| 触发砸单 SOL | ${avg(wins,'trigger_max_sell_sol').toFixed(2)} | ${avg(losses,'trigger_max_sell_sol').toFixed(2)} |\n`;
        md += `| 触发跌幅 % | ${avg(wins,'trigger_max_impact_pct').toFixed(1)} | ${avg(losses,'trigger_max_impact_pct').toFixed(1)} |\n`;
        md += `| 池子 SOL | ${avg(wins,'pool_quote_sol').toFixed(1)} | ${avg(losses,'pool_quote_sol').toFixed(1)} |\n`;
        md += `| FDV (k$) | ${(avg(wins,'fdv_usd')/1000).toFixed(1)} | ${(avg(losses,'fdv_usd')/1000).toFixed(1)} |\n`;
        md += `| dump→buy slot | ${avg(wins,'dump_to_buy_slot').toFixed(1)} | ${avg(losses,'dump_to_buy_slot').toFixed(1)} |\n`;
        md += `| 持仓秒数 | ${(avg(wins,'hold_ms')/1000).toFixed(0)} | ${(avg(losses,'hold_ms')/1000).toFixed(0)} |\n`;
        md += `| 平均 PnL % | ${avg(wins,'pnl_pct').toFixed(1)} | ${avg(losses,'pnl_pct').toFixed(1)} |\n\n`;
      }
    }

    // ============ Post-exit 分析(如果 v3.17.31 已上) ============
    if (postExitAnalysis && postExitAnalysis.length > 0) {
      md += `## 🔮 平仓后 5 分钟走势(我们卖得对吗?)\n\n`;
      md += `*post_pump > 10% 比例高 = 卖太早;post_dump < -20% = 卖对了避免大跌*\n\n`;
      md += `| 退出原因 | 笔数 | 卖后均涨% | 卖后均跌% | 卖太早率 |\n`;
      md += `|---|---|---|---|---|\n`;
      for (const r of postExitAnalysis) {
        md += `| ${r.exit_reason} | ${r.n} | ${r.avg_post_pump > 0 ? '+' : ''}${r.avg_post_pump}% | ${r.avg_post_dump}% | ${r.pct_sold_too_early}% |\n`;
      }
      md += `\n`;
    }

    // ============ 持仓明细(折叠) ============
    md += `## 💼 持仓明细\n\n`;
    md += `<details><summary>点击展开 (共 ${closed.length} 笔)</summary>\n\n`;
    if (closed.length === 0) {
      md += `无平仓记录。\n`;
    } else {
      md += `| 时间 | 代币 | 入场价 | 出场价 | 入场SOL | 出场SOL | PnL SOL | PnL % | 峰值% | 退出原因 |\n`;
      md += `|---|---|---|---|---|---|---|---|---|---|\n`;
      for (const p of closed) {
        const t = bjtIsoString(new Date(p.opened_at)).slice(11, 19);
        md += `| ${t} | ${p.symbol || p.mint.slice(0, 6)} | ${(p.entry_price ?? 0).toExponential(3)} | ${(p.exit_price ?? 0).toExponential(3)} | ${(p.entry_sol ?? 0).toFixed(3)} | ${(p.exit_sol ?? 0).toFixed(3)} | ${(p.pnl_sol ?? 0).toFixed(4)} | ${(p.pnl_pct ?? 0).toFixed(1)}% | ${(p.peak_pnl_pct ?? 0).toFixed(1)}% | ${p.exit_reason || '-'} |\n`;
      }
    }
    md += `\n</details>\n\n`;

    // ============ 信号日志(折叠) ============
    md += `## 📜 信号日志\n\n`;
    md += `<details><summary>点击展开 (共 ${signals.length} 条)</summary>\n\n`;
    md += `| 时间 | 代币 | 类型 | 卖出SOL | 跌幅% | 接受 | 备注 |\n`;
    md += `|---|---|---|---|---|---|---|\n`;
    for (const s of signals.slice(0, 500)) { // 限制 500 条防止巨型 MD
      const t = bjtIsoString(new Date(s.ts)).slice(11, 19);
      md += `| ${t} | ${s.symbol || (s.mint || '').slice(0, 6)} | ${s.kind} | ${(s.sell_sol ?? 0).toFixed(2)} | ${(s.price_impact_pct ?? 0).toFixed(2)} | ${s.accepted ? '✅' : '❌'} | ${(s.notes || s.reject_reason || '').slice(0, 80)} |\n`;
    }
    if (signals.length > 500) md += `\n*... 还有 ${signals.length - 500} 条已省略 ...*\n`;
    md += `\n</details>\n\n`;

    md += `---\n*由 dump-sniper v3.17.32 自动生成 · 北京时间 ${bjtIsoString()}*\n`;
    return md;
  }
}

module.exports = DailyReport;
