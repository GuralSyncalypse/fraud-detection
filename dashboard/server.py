from __future__ import annotations

import argparse
import csv
import cgi
import io
import json
import mimetypes
import subprocess
import tempfile
import threading
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


PAGE = r'''<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Fraud Detection Dashboard</title>
  <style>
    :root { --bg:#07111f; --panel:#0d1b2d; --panel2:#10243b; --line:#1d3854; --text:#e8f1fb; --muted:#8da6bf; --cyan:#49d7e8; --green:#45dda0; --amber:#ffc857; --red:#ff6b7a; }
    * { box-sizing:border-box; }
    body { margin:0; color:var(--text); background:radial-gradient(circle at 20% 0%, #123052 0, var(--bg) 45%); font:14px/1.5 Inter, Segoe UI, sans-serif; }
    .shell { max-width:1440px; margin:auto; padding:28px; }
    header { display:flex; justify-content:space-between; align-items:end; gap:20px; margin-bottom:24px; }
    .eyebrow { color:var(--cyan); font-size:11px; font-weight:700; letter-spacing:.18em; text-transform:uppercase; }
    h1 { margin:5px 0 0; font-size:clamp(27px,4vw,46px); letter-spacing:-.04em; }
    h2 { margin:0 0 16px; font-size:18px; }
    .subtitle { margin:8px 0 0; color:var(--muted); }
    button { border:1px solid #2c6a86; border-radius:10px; padding:10px 14px; color:var(--text); background:#123149; cursor:pointer; font-weight:700; }
    button:hover { background:#174362; }
    .status { margin:0 0 18px; color:var(--muted); }
    .status.ok { color:var(--green); }
    .status.error { color:var(--red); }
    .quick-nav { display:flex; flex-wrap:wrap; gap:8px; margin:-2px 0 18px; }
    .quick-nav a { padding:7px 11px; border:1px solid var(--line); border-radius:999px; color:var(--muted); background:#0a192a; font-size:12px; font-weight:700; text-decoration:none; transition:color .18s ease,border-color .18s ease,background .18s ease; }
    .quick-nav a:hover { color:var(--text); border-color:#347e9e; background:#123149; }
    .risk-summary { display:grid; grid-template-columns:auto 1fr auto auto; align-items:center; gap:16px; margin-bottom:18px; padding:17px 20px; border:1px solid #725b2b; border-radius:16px; background:linear-gradient(110deg,#332a16,#17243a 72%); box-shadow:0 12px 30px #0003; }
    .risk-summary.normal { border-color:#236b59; background:linear-gradient(110deg,#102f2c,#10243b 72%); }
    .risk-mark { display:grid; place-items:center; width:42px; height:42px; border-radius:14px; color:#271b00; background:var(--amber); font-size:22px; font-weight:900; }
    .risk-summary.normal .risk-mark { color:#06251b; background:var(--green); }
    .risk-copy h2 { margin:3px 0 0; font-size:20px; }
    .risk-copy p { margin:4px 0 0; color:var(--muted); }
    .risk-stat { min-width:120px; padding-left:16px; border-left:1px solid #ffffff22; }
    .risk-stat span { display:block; color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.07em; }
    .risk-stat strong { display:block; margin-top:3px; color:var(--text); font-size:23px; }
    .risk-state { padding:6px 10px; border:1px solid #a57b25; border-radius:999px; color:#ffd978; font-size:11px; font-weight:900; letter-spacing:.08em; white-space:nowrap; }
    .risk-summary.normal .risk-state { border-color:#2e8d70; color:#8df0c1; }
    .upload-panel { display:grid; grid-template-columns:1fr auto; align-items:center; gap:20px; margin-bottom:18px; padding:18px 20px; border:1px solid #2a617e; border-radius:16px; background:linear-gradient(110deg,#0d2941,#0b1c30); box-shadow:0 12px 30px #0003; }
    .upload-copy h2 { margin:2px 0 3px; font-size:19px; }
    .upload-copy p { margin:0; color:var(--muted); font-size:13px; }
    .upload-form { display:flex; align-items:center; justify-content:flex-end; flex-wrap:wrap; gap:9px; }
    .file-picker { display:flex; align-items:center; min-height:40px; max-width:260px; padding:0 11px; overflow:hidden; border:1px dashed #347e9e; border-radius:10px; color:var(--muted); background:#071827; cursor:pointer; }
    .file-picker:hover { border-color:var(--cyan); color:var(--text); }
    .file-picker input { width:0; height:0; opacity:0; }
    .upload-form button[disabled] { opacity:.55; cursor:wait; }
    .template-link { color:var(--cyan); font-size:12px; font-weight:700; text-decoration:none; white-space:nowrap; }
    .template-link:hover { text-decoration:underline; }
    .upload-status { grid-column:1 / -1; min-height:18px; color:var(--muted); font-size:12px; }
    .upload-status.success { color:var(--green); }
    .upload-status.error { color:var(--red); }
    .cards { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:14px; margin-bottom:18px; }
    .card, .panel { border:1px solid var(--line); border-radius:16px; background:linear-gradient(145deg,rgba(16,36,59,.96),rgba(10,24,40,.96)); box-shadow:0 12px 30px #0003; }
    .card { position:relative; overflow:hidden; padding:18px; min-height:118px; }
    .card::before { content:''; position:absolute; inset:0 0 auto; height:3px; background:var(--cyan); opacity:.9; }
    .card:nth-child(2)::before { background:#9aa9ff; }
    .card:nth-child(3)::before { background:var(--amber); }
    .card:nth-child(4)::before { background:var(--green); }
    .label { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.08em; }
    .value { margin-top:12px; font-size:29px; font-weight:800; letter-spacing:-.04em; }
    .accent { color:var(--cyan); }
    .good { color:var(--green); }
    .warn { color:var(--amber); }
    .layout { display:grid; grid-template-columns:1.2fr .8fr; gap:18px; }
    .panel { padding:20px; margin-bottom:18px; }
    .wide { grid-column:1 / -1; }
    .model-table { width:100%; border-collapse:collapse; }
    th, td { padding:11px 8px; border-bottom:1px solid var(--line); text-align:left; white-space:nowrap; }
    th { position:sticky; top:0; z-index:1; color:var(--muted); background:#0a192a; font-size:11px; text-transform:uppercase; letter-spacing:.06em; }
    tbody tr { transition:background .15s ease; }
    tbody tr:hover { background:#122d45; }
    td.num { text-align:right; font-variant-numeric:tabular-nums; }
    tr.selected { background:#15334a; }
    .pill { display:inline-flex; align-items:center; gap:6px; border-radius:999px; padding:5px 9px; color:#07111f; background:var(--green); font-size:12px; font-weight:800; }
    .pill.warn { color:#271b00; background:var(--amber); }
    .meta { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }
    .meta div { padding:13px; border:1px solid var(--line); border-radius:12px; background:#0a192a; }
    .meta strong { display:block; margin-top:4px; font-size:18px; }
    .panel-tools { display:flex; align-items:center; flex-wrap:wrap; gap:10px; margin:-4px 0 14px; }
    .search-box, .filter-select { display:flex; align-items:center; gap:8px; min-height:38px; padding:0 11px; border:1px solid var(--line); border-radius:10px; color:var(--muted); background:#0a192a; }
    .search-box { flex:1 1 260px; }
    .search-box input, .filter-select select { border:0; outline:0; color:var(--text); background:transparent; font:inherit; }
    .search-box input { width:100%; }
    .search-box input::placeholder { color:#66819b; }
    .filter-select select { cursor:pointer; }
    .filter-select option { color:#10243b; background:#fff; }
    .tool-result { margin-left:auto; color:var(--muted); font-size:12px; font-weight:700; }
    .figures { display:grid; grid-template-columns:minmax(0,1fr); gap:20px; }
    figure { margin:0; padding:18px; border:1px solid var(--line); border-radius:18px; background:linear-gradient(145deg,#0d2137,#091727); transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease; }
    figure:hover { transform:translateY(-2px); border-color:#347e9e; box-shadow:0 16px 34px #0005; }
    figure img { display:block; width:100%; height:auto; min-height:320px; aspect-ratio:16/9; object-fit:contain; border-radius:12px; background:#f7fbff; }
    figcaption { display:flex; align-items:flex-start; justify-content:space-between; gap:16px; padding:0 2px 14px; color:var(--text); }
    .figure-copy { min-width:0; }
    .figure-title { display:block; font-size:clamp(19px,2.2vw,28px); line-height:1.15; font-weight:800; letter-spacing:-.025em; }
    figcaption small { display:block; padding-top:6px; color:var(--muted); font-size:14px; line-height:1.45; font-weight:400; }
    .figure-index { flex:none; padding:5px 9px; border:1px solid #2a617e; border-radius:999px; color:var(--cyan); font-size:11px; font-weight:800; letter-spacing:.08em; }
    figure { position:relative; cursor:zoom-in; }
    figure::after { content:'Nhấn để phóng to'; position:absolute; right:28px; bottom:30px; padding:8px 11px; border:1px solid #2a617e; border-radius:999px; color:#dffaff; background:#07111fcc; font-size:12px; font-weight:700; opacity:0; transform:translateY(5px); transition:opacity .18s ease,transform .18s ease; pointer-events:none; }
    figure:hover::after, figure:focus-visible::after { opacity:1; transform:translateY(0); }
    figure:focus-visible { outline:2px solid var(--cyan); outline-offset:4px; }
    body.modal-open { overflow:hidden; }
    .lightbox { position:fixed; inset:0; z-index:20; display:none; align-items:center; justify-content:center; padding:24px; background:#020914e8; backdrop-filter:blur(10px); }
    .lightbox.is-open { display:flex; }
    .lightbox-card { width:min(1500px,96vw); max-height:94vh; padding:18px; border:1px solid #3b7892; border-radius:20px; background:linear-gradient(145deg,#102b45,#071625); box-shadow:0 24px 80px #000b; }
    .lightbox-head { display:flex; align-items:flex-start; justify-content:space-between; gap:18px; padding:0 2px 14px; }
    .lightbox-title { color:var(--text); font-size:clamp(20px,2.4vw,32px); font-weight:800; letter-spacing:-.025em; }
    .lightbox-subtitle { padding-top:5px; color:var(--muted); font-size:14px; }
    .lightbox-close { flex:none; width:40px; height:40px; padding:0; border:1px solid #3b7892; border-radius:50%; color:var(--text); background:#123149; font-size:24px; line-height:1; }
    .lightbox-close:hover { background:#1b5270; }
    .lightbox img { display:block; width:100%; max-height:calc(94vh - 104px); object-fit:contain; border-radius:12px; background:#f7fbff; }
    .clickable-row { cursor:pointer; }
    .clickable-row:focus-visible { outline:2px solid var(--cyan); outline-offset:-2px; }
    .transaction-detail { position:fixed; inset:0; z-index:19; display:none; align-items:center; justify-content:center; padding:24px; background:#020914e8; backdrop-filter:blur(10px); }
    .transaction-detail.is-open { display:flex; }
    .transaction-card { width:min(720px,96vw); max-height:92vh; overflow:auto; padding:22px; border:1px solid #3b7892; border-radius:20px; background:linear-gradient(145deg,#102b45,#071625); box-shadow:0 24px 80px #000b; }
    .transaction-head { display:flex; align-items:flex-start; justify-content:space-between; gap:18px; }
    .transaction-title { color:var(--text); font-size:24px; font-weight:800; }
    .transaction-subtitle { margin-top:4px; color:var(--muted); }
    .transaction-close { flex:none; width:40px; height:40px; padding:0; border:1px solid #3b7892; border-radius:50%; color:var(--text); background:#123149; font-size:24px; line-height:1; }
    .detail-score { margin:22px 0 18px; padding:16px; border:1px solid #725b2b; border-radius:14px; background:#332a16; }
    .detail-score-head { display:flex; justify-content:space-between; gap:12px; color:#ffd978; font-weight:800; }
    .detail-score-track { height:9px; margin-top:12px; overflow:hidden; border-radius:99px; background:#59471f; }
    .detail-score-fill { width:0; height:100%; border-radius:inherit; background:linear-gradient(90deg,#ffc857,#ff6b7a); transition:width .25s ease; }
    .detail-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }
    .detail-item { padding:13px; border:1px solid var(--line); border-radius:12px; background:#0a192a; }
    .detail-item span { display:block; color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.07em; }
    .detail-item strong { display:block; margin-top:4px; color:var(--text); font-size:17px; }
    .detail-note { margin:18px 0 0; color:var(--muted); font-size:13px; }
    .rows { overflow:auto; max-height:390px; }
    .empty { color:var(--muted); padding:20px 0; }
    @media (max-width:900px) { .cards { grid-template-columns:repeat(2,minmax(0,1fr)); } .layout { grid-template-columns:1fr; } .risk-summary { grid-template-columns:auto 1fr auto; } .risk-state { grid-column:2 / -1; justify-self:start; } .upload-panel { grid-template-columns:1fr; } .upload-form { justify-content:flex-start; } }
    @media (max-width:560px) { .shell { padding:18px; } header { align-items:start; flex-direction:column; } .cards { grid-template-columns:1fr; } .figures { grid-template-columns:1fr; } .risk-summary { grid-template-columns:auto 1fr; } .risk-stat { grid-column:2; padding-left:0; border-left:0; } .risk-state { grid-column:2; } .upload-form { align-items:stretch; flex-direction:column; } .file-picker { max-width:none; } }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <div class="eyebrow">HỆ THỐNG PHÁT HIỆN GIAN LẬN / GIAO DIỆN DỰ ÁN</div>
        <h1>Trung tâm giám sát giao dịch</h1>
        <p class="subtitle">Theo dõi dữ liệu, hiệu năng mô hình và các giao dịch cần kiểm tra.</p>
      </div>
      <button id="refresh">↻ Làm mới dữ liệu</button>
    </header>
    <div id="status" class="status">Đang tải dữ liệu...</div>
    <nav class="quick-nav" aria-label="Điều hướng nhanh"><a href="#overview">Tổng quan</a><a href="#suspicious">Giao dịch cần kiểm tra</a><a href="#figures">Biểu đồ phân tích</a></nav>
    <section id="risk-summary" class="risk-summary" aria-live="polite">
      <div class="risk-mark">!</div>
      <div class="risk-copy"><div class="label">Tóm tắt rủi ro</div><h2 id="risk-title">Đang phân tích dữ liệu...</h2><p id="risk-detail">Đang tải danh sách ưu tiên.</p></div>
      <div class="risk-stat"><span>Danh sách ưu tiên</span><strong id="risk-count">—</strong></div>
      <div id="risk-state" class="risk-state">ĐANG TẢI</div>
    </section>
    <section class="upload-panel">
      <div class="upload-copy"><div class="label">Dự đoán dữ liệu mới</div><h2>Upload CSV để kiểm tra giao dịch</h2><p>Dùng đúng các cột mẫu và hệ thống sẽ chạy model đang được chọn.</p></div>
      <form id="upload-form" class="upload-form"><label class="file-picker"><span id="file-label">Chọn file CSV</span><input id="prediction-file" type="file" accept=".csv,text/csv"></label><button id="upload-submit" type="submit">Chạy dự đoán</button><a class="template-link" href="/api/prediction-template" download="prediction_template.csv">Tải file mẫu</a></form>
      <div id="upload-status" class="upload-status" aria-live="polite"></div>
    </section>
    <section id="overview" class="cards">
      <article class="card"><div class="label">Giao dịch sạch</div><div id="clean" class="value accent">—</div></article>
      <article class="card"><div class="label">Giao dịch có nhãn</div><div id="labeled" class="value">—</div></article>
      <article class="card"><div class="label">Tỷ lệ gian lận</div><div id="fraud-rate" class="value warn">—</div></article>
      <article class="card"><div class="label">Gian lận đã biết</div><div id="fraud-count" class="value good">—</div></article>
    </section>
    <div class="layout">
      <section class="panel">
        <h2>Đánh giá mô hình</h2>
        <div id="selection" class="meta"></div>
        <div class="rows"><table class="model-table"><thead><tr><th>Mô hình</th><th>Cách xử lý lệch lớp</th><th>PR-AUC</th><th>Độ chính xác</th><th>Độ bao phủ</th><th>F1</th></tr></thead><tbody id="models"></tbody></table></div>
      </section>
      <section class="panel">
        <h2>Dự đoán mới nhất</h2>
        <div id="prediction-meta" class="meta"></div>
        <div class="rows"><table class="model-table"><thead><tr><th>Mã giao dịch</th><th>Số tiền</th><th>Xác suất gian lận</th><th>Kết quả</th></tr></thead><tbody id="predictions"></tbody></table></div>
      </section>
      <section class="panel wide">
        <h2>Các giao dịch cần kiểm tra</h2>
        <div class="panel-tools"><label class="search-box"><span aria-hidden="true">⌕</span><input id="suspicious-search" type="search" placeholder="Tìm mã giao dịch hoặc thời gian" autocomplete="off"></label><label class="filter-select"><span>Điểm tối thiểu</span><select id="risk-filter"><option value="0">Tất cả</option><option value="0.2">≥ 0.20</option><option value="0.5">≥ 0.50</option><option value="0.7">≥ 0.70</option></select></label><span id="suspicious-result-count" class="tool-result">Đang tải...</span></div>
        <div class="rows"><table class="model-table"><thead><tr><th>Mã giao dịch</th><th>Thời gian</th><th>Số tiền</th><th>Xác suất gian lận</th><th>Kết quả</th><th>Nhãn thực tế</th></tr></thead><tbody id="suspicious"></tbody></table></div>
      </section>
      <section class="panel wide">
        <h2>Biểu đồ phân tích</h2>
        <div id="figures" class="figures"></div>
      </section>
    </div>
  </div>
  <div id="lightbox" class="lightbox" aria-hidden="true">
    <div class="lightbox-card" role="dialog" aria-modal="true" aria-labelledby="lightbox-title">
      <div class="lightbox-head">
        <div><div id="lightbox-title" class="lightbox-title">Xem biểu đồ</div><div id="lightbox-subtitle" class="lightbox-subtitle"></div></div>
        <button id="lightbox-close" class="lightbox-close" type="button" aria-label="Đóng ảnh">×</button>
      </div>
      <img id="lightbox-image" alt="">
    </div>
  </div>
  <div id="transaction-detail" class="transaction-detail" aria-hidden="true">
    <div class="transaction-card" role="dialog" aria-modal="true" aria-labelledby="transaction-title">
      <div class="transaction-head"><div><div id="transaction-title" class="transaction-title">Chi tiết giao dịch</div><div id="transaction-subtitle" class="transaction-subtitle"></div></div><button id="transaction-detail-close" class="transaction-close" type="button" aria-label="Đóng chi tiết">×</button></div>
      <div class="detail-score"><div class="detail-score-head"><span>Điểm rủi ro</span><span id="detail-score-value">0.0000</span></div><div class="detail-score-track"><div id="detail-score-fill" class="detail-score-fill"></div></div></div>
      <div id="transaction-detail-grid" class="detail-grid"></div>
      <p class="detail-note">Điểm rủi ro vượt ngưỡng là tín hiệu cần ưu tiên kiểm tra, không phải kết luận cuối cùng.</p>
    </div>
  </div>
  <script>
    const $ = id => document.getElementById(id);
    const number = value => new Intl.NumberFormat('vi-VN').format(Number(value || 0));
    const percent = value => `${(Number(value || 0) * 100).toFixed(2)}%`;
    const amount = value => Number(value || 0).toLocaleString('vi-VN', {minimumFractionDigits: 2, maximumFractionDigits: 2});
    const probability = value => Number(value || 0).toFixed(4);
    const cell = (value, cls='') => `<td class="${cls}">${value ?? '—'}</td>`;
    const modelNames = {
      lr_none: 'Logistic Regression · không cân bằng',
      lr_class_weight: 'Logistic Regression · trọng số lớp',
      rf_none: 'Random Forest · không cân bằng',
      rf_class_weight: 'Random Forest · trọng số lớp',
      rf_undersampling: 'Random Forest · giảm mẫu lớp bình thường',
      isolation_forest: 'Isolation Forest · không giám sát'
    };
    const methodNames = {
      None: 'Không cân bằng lớp',
      'Class Weight': 'Gán trọng số lớp',
      Undersampling: 'Giảm mẫu lớp bình thường',
      Unsupervised: 'Không giám sát'
    };
    const figureInfo = {
      '01_class_distribution.png': ['Phân bố giao dịch', 'So sánh giao dịch bình thường và gian lận'],
      '02_amount_distribution.png': ['Phân bố số tiền giao dịch', 'Khoảng tiền phổ biến trong dữ liệu'],
      '03_fraud_amount_distribution.png': ['Số tiền của giao dịch gian lận', 'Phân bố riêng nhóm đã có nhãn gian lận'],
      '04_confusion_matrix.png': ['Ma trận nhầm lẫn', 'Mô hình dự đoán đúng và nhầm ở đâu'],
      '05_metric_comparison.png': ['So sánh hiệu năng mô hình', 'Độ chính xác, độ bao phủ và F1'],
      '06_precision_recall_curve.png': ['Đường Precision – Recall', 'Trade-off giữa bắt đúng và bắt đủ gian lận'],
      '07_rf_feature_importance.png': ['Yếu tố ảnh hưởng đến mô hình', 'Độ quan trọng của các feature, không phải quan hệ nhân quả'],
      '08_probability_distribution.png': ['Phân bố điểm rủi ro', 'Các giao dịch tập trung ở mức xác suất nào'],
      '09_threshold_analysis.png': ['Ảnh hưởng của ngưỡng cảnh báo', 'Chọn ngưỡng theo mục tiêu vận hành'],
      '10_top_suspicious_transactions.png': ['Top giao dịch cần kiểm tra', 'Các giao dịch có điểm rủi ro cao nhất']
    };
    const modelLabel = slug => modelNames[slug] || slug || '—';
    const methodLabel = method => methodNames[method] || method || '—';
    const statusLabel = status => ({NORMAL:'Bình thường', NEEDS_REVIEW:'Cần kiểm tra'})[status] || status || '—';
    const actualLabel = value => value === '1' ? 'Gian lận' : value === '0' ? 'Bình thường' : '—';
    const statusBadge = status => `<span class="pill ${status === 'NEEDS_REVIEW' ? 'warn' : ''}">${statusLabel(status)}</span>`;
    let suspiciousRows = [];
    const suspiciousColumns = [{key:'transaction_id'},{key:'transaction_time'},{key:'amount',cls:'num',render:row => amount(row.amount)},{key:'fraud_probability',cls:'num',render:row => probability(row.fraud_probability)},{key:'status',render:row => statusBadge(row.status)},{key:'actual_class',cls:'num',render:row => actualLabel(row.actual_class)}];
    const predictionColumns = [{key:'transaction_id'},{key:'amount',cls:'num',render:row => amount(row.amount)},{key:'fraud_probability',cls:'num',render:row => probability(row.fraud_probability)},{key:'status',render:row => statusBadge(row.status)}];
    function safe(value) { return String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
    function renderRows(id, rows, columns, empty='Chưa có dữ liệu') {
      const body = $(id);
      if (!rows?.length) { body.innerHTML = `<tr><td colspan="${columns.length}" class="empty">${empty}</td></tr>`; return; }
      body.innerHTML = rows.map(row => `<tr>${columns.map(column => cell(column.render ? column.render(row) : safe(row[column.key]), column.cls || '')).join('')}</tr>`).join('');
    }
    function renderPrediction(prediction) {
      const rows = prediction.rows || [];
      $('prediction-meta').innerHTML = `<div><span class="label">Số giao dịch</span><strong>${number(prediction.input_rows ?? rows.length)}</strong></div><div><span class="label">Bị gắn cờ</span><strong class="${prediction.flagged_rows ? 'warn' : 'good'}">${number(prediction.flagged_rows)}</strong></div><div><span class="label">Mô hình</span><strong>${safe(modelLabel(prediction.model))}</strong></div><div><span class="label">Ngưỡng cảnh báo</span><strong>${Number(prediction.threshold || 0).toFixed(2)}</strong></div>`;
      renderRows('predictions', rows, predictionColumns, 'Chưa có kết quả dự đoán');
    }
    function renderSuspicious(rows) {
      suspiciousRows = rows || [];
      const query = $('suspicious-search').value.trim().toLowerCase();
      const minimumRisk = Number($('risk-filter').value || 0);
      const filtered = suspiciousRows.filter(row => {
        const searchable = `${row.transaction_id || ''} ${row.transaction_time || ''}`.toLowerCase();
        return searchable.includes(query) && Number(row.fraud_probability || 0) >= minimumRisk;
      });
      renderRows('suspicious', filtered, suspiciousColumns, 'Không có giao dịch phù hợp');
      $('suspicious-result-count').textContent = `${number(filtered.length)} / ${number(suspiciousRows.length)} giao dịch`;
      document.querySelectorAll('#suspicious tr').forEach((tableRow, index) => {
        if (!filtered[index]) return;
        tableRow.classList.add('clickable-row');
        tableRow.tabIndex = 0;
        tableRow.addEventListener('click', () => openTransaction(filtered[index]));
        tableRow.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openTransaction(filtered[index]); } });
      });
    }
    function openTransaction(row) {
      const score = Math.max(0, Math.min(1, Number(row.fraud_probability || 0)));
      $('transaction-subtitle').textContent = `Mã ${row.transaction_id || '—'} · ${row.transaction_time || '—'}`;
      $('detail-score-value').textContent = probability(score);
      $('detail-score-fill').style.width = `${score * 100}%`;
      const item = (label, value) => `<div class="detail-item"><span>${label}</span><strong>${value}</strong></div>`;
      $('transaction-detail-grid').innerHTML = [
        item('Mã giao dịch', safe(row.transaction_id)),
        item('Thời gian', safe(row.transaction_time)),
        item('Số tiền', safe(amount(row.amount))),
        item('Kết quả', statusBadge(row.status)),
        item('Nhãn thực tế', safe(actualLabel(row.actual_class))),
        item('Mô hình', safe(modelLabel(row.model)))
      ].join('');
      $('transaction-detail').classList.add('is-open');
      $('transaction-detail').setAttribute('aria-hidden', 'false');
      document.body.classList.add('modal-open');
      $('transaction-detail-close').focus();
    }
    function closeTransaction() {
      $('transaction-detail').classList.remove('is-open');
      $('transaction-detail').setAttribute('aria-hidden', 'true');
      document.body.classList.remove('modal-open');
    }
    function openFigure(figure) {
      const image = figure.querySelector('img');
      $('lightbox-image').src = image.src;
      $('lightbox-image').alt = image.alt;
      $('lightbox-title').textContent = figure.querySelector('.figure-title')?.textContent || 'Xem biểu đồ';
      $('lightbox-subtitle').textContent = figure.querySelector('small')?.textContent || '';
      $('lightbox').classList.add('is-open');
      $('lightbox').setAttribute('aria-hidden', 'false');
      document.body.classList.add('modal-open');
      $('lightbox-close').focus();
    }
    function closeFigure() {
      $('lightbox').classList.remove('is-open');
      $('lightbox').setAttribute('aria-hidden', 'true');
      $('lightbox-image').removeAttribute('src');
      document.body.classList.remove('modal-open');
    }
    function bindFigureInteractions() {
      document.querySelectorAll('#figures figure').forEach(figure => {
        figure.addEventListener('click', () => openFigure(figure));
        figure.addEventListener('keydown', event => {
          if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); openFigure(figure); }
        });
      });
    }
    $('lightbox-close').addEventListener('click', closeFigure);
    $('lightbox').addEventListener('click', event => { if (event.target === $('lightbox')) closeFigure(); });
    $('transaction-detail-close').addEventListener('click', closeTransaction);
    $('transaction-detail').addEventListener('click', event => { if (event.target === $('transaction-detail')) closeTransaction(); });
    document.addEventListener('keydown', event => { if (event.key !== 'Escape') return; if ($('lightbox').classList.contains('is-open')) closeFigure(); if ($('transaction-detail').classList.contains('is-open')) closeTransaction(); });
    $('suspicious-search').addEventListener('input', () => renderSuspicious(suspiciousRows));
    $('risk-filter').addEventListener('change', () => renderSuspicious(suspiciousRows));
    function render(data) {
      const tx = data.transactions;
      $('clean').textContent = number(tx.clean);
      $('labeled').textContent = number(tx.labeled);
      $('fraud-rate').textContent = `${Number(tx.fraud_rate || 0).toFixed(4)}%`;
      $('fraud-count').textContent = number(tx.fraud);
      const selection = data.selection || {};
      const suspicious = data.suspicious || [];
      const threshold = Number(selection.recommended_threshold || 0);
      const maxRisk = suspicious.reduce((highest, row) => Math.max(highest, Number(row.fraud_probability || 0)), 0);
      const hasRisk = suspicious.length > 0;
      $('risk-summary').className = `risk-summary ${hasRisk ? 'warning' : 'normal'}`;
      $('risk-title').textContent = hasRisk ? `Có ${number(suspicious.length)} giao dịch cần ưu tiên kiểm tra` : 'Chưa có giao dịch cần kiểm tra';
      $('risk-detail').textContent = hasRisk ? `Điểm rủi ro cao nhất: ${probability(maxRisk)} · ngưỡng cảnh báo: ${threshold.toFixed(2)}` : `Ngưỡng cảnh báo hiện tại: ${threshold.toFixed(2)}`;
      $('risk-count').textContent = number(suspicious.length);
      $('risk-state').textContent = hasRisk ? 'CẦN KIỂM TRA' : 'ỔN ĐỊNH';
      $('selection').innerHTML = `<div><span class="label">Mô hình đề xuất</span><strong class="accent">${safe(modelLabel(selection.recommended_model))}</strong></div><div><span class="label">Ngưỡng cảnh báo</span><strong class="warn">${Number(selection.recommended_threshold || 0).toFixed(2)}</strong></div><div><span class="label">Tiêu chí chọn</span><strong>PR-AUC trên validation</strong></div><div><span class="label">Số dòng kiểm thử</span><strong>${number(data.test_rows)}</strong></div>`;
      $('models').innerHTML = (data.models || []).map(model => `<tr class="${model.slug === selection.recommended_model ? 'selected' : ''}">${cell(safe(modelLabel(model.slug)))}${cell(safe(methodLabel(model.imbalance_method)))}${cell(Number(model.pr_auc || 0).toFixed(4), 'num')}${cell(percent(model.precision), 'num')}${cell(percent(model.recall), 'num')}${cell(percent(model.f1), 'num')}</tr>`).join('');
      const prediction = data.prediction || {};
      renderPrediction(prediction);
      renderSuspicious(data.suspicious);
      $('figures').innerHTML = (data.figures || []).map((name, index) => { const info = figureInfo[name] || [name, '']; const chartNumber = String(index + 1).padStart(2, '0'); return `<figure tabindex="0" role="button" aria-label="Nhấn để phóng to biểu đồ ${chartNumber}"><figcaption><div class="figure-copy"><span class="figure-title">${safe(info[0])}</span><small>${safe(info[1])}</small></div><span class="figure-index">#${chartNumber}</span></figcaption><img loading="lazy" src="/figures/${encodeURIComponent(name)}" alt="${safe(info[0])}"></figure>`; }).join('');
      bindFigureInteractions();
      $('status').textContent = `Đã cập nhật ${safe(data.updated_at)} · tự làm mới mỗi 10 giây`;
      $('status').className = 'status ok';
    }
    async function uploadPrediction(event) {
      event.preventDefault();
      const file = $('prediction-file').files[0];
      if (!file) { $('upload-status').textContent = 'Hãy chọn file CSV trước khi chạy.'; $('upload-status').className = 'upload-status error'; return; }
      const form = new FormData();
      form.append('file', file);
      $('upload-submit').disabled = true;
      $('upload-status').textContent = 'Đang chạy model trên file CSV...';
      $('upload-status').className = 'upload-status';
      try {
        const response = await fetch('/api/predict-upload', {method:'POST', body:form});
        const result = await response.json();
        if (!response.ok || !result.ok) throw new Error(result.error || `HTTP ${response.status}`);
        renderPrediction(result.prediction || {});
        $('upload-status').textContent = `Đã dự đoán ${number(result.prediction.input_rows)} giao dịch từ ${result.filename} · gắn cờ ${number(result.prediction.flagged_rows)} giao dịch.`;
        $('upload-status').className = 'upload-status success';
      } catch (error) {
        $('upload-status').textContent = `Không thể dự đoán: ${error.message}`;
        $('upload-status').className = 'upload-status error';
      } finally {
        $('upload-submit').disabled = false;
      }
    }
    $('prediction-file').addEventListener('change', () => { $('file-label').textContent = $('prediction-file').files[0]?.name || 'Chọn file CSV'; });
    $('upload-form').addEventListener('submit', uploadPrediction);
    async function load() {
      try { const response = await fetch(`/api/dashboard?t=${Date.now()}`); if (!response.ok) throw new Error(`HTTP ${response.status}`); render(await response.json()); }
      catch (error) { $('status').textContent = `Không đọc được kết quả: ${error.message}`; $('status').className = 'status error'; }
    }
    $('refresh').addEventListener('click', load);
    load();
    setInterval(load, 10000);
  </script>
</body>
</html>'''


MAX_UPLOAD_BYTES = 25 * 1024 * 1024
REQUIRED_UPLOAD_COLUMNS = {
    "transaction_id",
    "transaction_time",
    "amount",
    "use_chip",
    "merchant_state",
    "mcc",
    "errors",
}
PREDICTION_TEMPLATE = (
    "transaction_id,transaction_time,amount,use_chip,merchant_state,mcc,errors\n"
    "9900000001,2019-12-31 23:30:00,1500.0,Online Transaction,,5732,\n"
)
PREDICT_LOCK = threading.Lock()


def read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def read_csv(path: Path, limit: int = 20) -> list[dict]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(__import__("itertools").islice(csv.DictReader(handle), limit))
    except (FileNotFoundError, OSError):
        return []


def parse_uploaded_csv(handler: BaseHTTPRequestHandler) -> tuple[bytes, str]:
    content_length = int(handler.headers.get("Content-Length", "0") or 0)
    if content_length <= 0:
        raise ValueError("Chưa nhận được file CSV")
    if content_length > MAX_UPLOAD_BYTES:
        raise ValueError("File quá lớn; giới hạn upload là 25 MB")
    content_type = handler.headers.get("Content-Type", "")
    if not content_type.startswith("multipart/form-data"):
        raise ValueError("Yêu cầu upload phải dùng multipart/form-data")
    form = cgi.FieldStorage(
        fp=handler.rfile,
        headers=handler.headers,
        environ={
            "REQUEST_METHOD": "POST",
            "CONTENT_TYPE": content_type,
            "CONTENT_LENGTH": str(content_length),
        },
    )
    field = form["file"] if "file" in form else None
    if isinstance(field, list):
        field = field[0]
    if field is None or not getattr(field, "filename", None):
        raise ValueError("Hãy chọn một file CSV")
    payload = field.file.read(MAX_UPLOAD_BYTES + 1)
    if len(payload) > MAX_UPLOAD_BYTES:
        raise ValueError("File quá lớn; giới hạn upload là 25 MB")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as error:
        raise ValueError("CSV phải dùng mã hóa UTF-8") from error
    fieldnames = set(csv.DictReader(io.StringIO(text)).fieldnames or [])
    missing = sorted(REQUIRED_UPLOAD_COLUMNS - fieldnames)
    if missing:
        raise ValueError(f"Thiếu cột bắt buộc: {', '.join(missing)}")
    return payload, Path(field.filename).name


def run_uploaded_prediction(payload: bytes) -> dict:
    job_id = uuid.uuid4().hex[:12]
    temp_dir = Path(tempfile.gettempdir())
    input_path = temp_dir / f"fraud-dashboard-upload-{job_id}.csv"
    report_path = temp_dir / f"fraud-dashboard-report-{job_id}.json"
    csv_path = temp_dir / f"fraud-dashboard-result-{job_id}.csv"
    output_path = f"hdfs:///financial/predictions/dashboard_upload_{job_id}"
    input_path.write_bytes(payload)
    command = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--driver-memory",
        "2g",
        "--conf",
        "spark.driver.host=dashboard",
        "--conf",
        "spark.driver.bindAddress=0.0.0.0",
        "--conf",
        "spark.sql.shuffle.partitions=16",
        "/workspace/src/predict.py",
        "--input",
        str(input_path),
        "--output",
        output_path,
        "--report",
        str(report_path),
        "--csv",
        str(csv_path),
    ]
    try:
        with PREDICT_LOCK:
            completed = subprocess.run(
                command,
                cwd="/workspace",
                capture_output=True,
                text=True,
                timeout=600,
                check=False,
            )
        if completed.returncode != 0:
            details = (completed.stderr or completed.stdout or "Không rõ lỗi").strip()
            raise RuntimeError(details[-2000:])
        report = read_json(report_path)
        if not report:
            raise RuntimeError("Pipeline không tạo được báo cáo dự đoán")
        return {
            "prediction": {
                **report,
                "rows": read_csv(csv_path, limit=100),
            }
        }
    finally:
        for path in (input_path, report_path, csv_path):
            path.unlink(missing_ok=True)


def dashboard_data(output_dir: Path) -> dict:
    results = output_dir / "results"
    phase2 = read_json(results / "phase2_summary.json")
    phase4 = read_json(results / "phase4_summary.json")
    prediction = read_json(results / "new_prediction_summary.json")
    phase5 = read_json(results / "phase5_summary.json")
    clean = phase2.get("clean_profile", {})
    distribution = clean.get("class_distribution", {})
    fraud = distribution.get("1", {})
    figures = phase5.get("figures", [])
    if not figures:
        figures = [path.name for path in sorted((output_dir / "figures").glob("*.png"))]
    mtimes = [path.stat().st_mtime for path in results.glob("*.json") if path.exists()]
    updated = datetime.fromtimestamp(max(mtimes)).astimezone().strftime("%Y-%m-%d %H:%M:%S") if mtimes else "—"
    return {
        "updated_at": updated,
        "transactions": {
            "raw": phase2.get("raw_profile", {}).get("row_count", 0),
            "clean": clean.get("clean_transaction_count", 0),
            "labeled": clean.get("labeled_transaction_count", 0),
            "fraud": fraud.get("count", 0),
            "fraud_rate": fraud.get("percentage", 0),
        },
        "selection": phase4.get("selection", {}),
        "test_rows": phase4.get("evaluation_scope", {}).get("rows", 0),
        "models": phase4.get("model_comparison", []),
        "prediction": {**prediction, "rows": read_csv(results / "new_transactions_predictions.csv")},
        "suspicious": read_csv(results / "top_suspicious_transactions.csv"),
        "figures": figures,
    }


class DashboardHandler(BaseHTTPRequestHandler):
    output_dir = Path("output")

    def send_bytes(self, body: bytes, content_type: str, status: int = 200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, payload: dict, status: int = 200) -> None:
        self.send_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            "application/json; charset=utf-8",
            status,
        )

    def do_GET(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == "/":
            self.send_bytes(PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/healthz":
            self.send_bytes(b"ok\n", "text/plain; charset=utf-8")
            return
        if path == "/api/prediction-template":
            self.send_bytes(PREDICTION_TEMPLATE.encode("utf-8"), "text/csv; charset=utf-8")
            return
        if path == "/api/dashboard":
            body = json.dumps(dashboard_data(self.output_dir), ensure_ascii=False).encode("utf-8")
            self.send_bytes(body, "application/json; charset=utf-8")
            return
        if path.startswith("/figures/"):
            name = Path(path.removeprefix("/figures/")).name
            figure = self.output_dir / "figures" / name
            if name != Path(path.removeprefix("/figures/")).name or figure.suffix.lower() != ".png" or not figure.is_file():
                self.send_error(404)
                return
            self.send_bytes(figure.read_bytes(), mimetypes.guess_type(name)[0] or "image/png")
            return
        self.send_error(404)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path != "/api/predict-upload":
            self.send_error(404)
            return
        try:
            payload, filename = parse_uploaded_csv(self)
            response = run_uploaded_prediction(payload)
            response["filename"] = filename
            self.send_json({"ok": True, **response})
        except ValueError as error:
            self.send_json({"ok": False, "error": str(error)}, status=400)
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            self.send_json({"ok": False, "error": str(error)}, status=500)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the fraud detection dashboard")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8501)
    parser.add_argument("--output", type=Path, default=Path("output"))
    args = parser.parse_args()
    DashboardHandler.output_dir = args.output
    server = ThreadingHTTPServer((args.host, args.port), DashboardHandler)
    print(f"[dashboard] listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
