// Paper evidence application. No remote chart dependency and no archival data merge.
// Each series belongs to exactly one experiment; comparisons never cross campaigns.
const KEY_ORDER = ['SEQUENTIAL', 'UUIDV1', 'UUIDV7', 'ULID', 'ULID_MONOTONIC', 'UUIDV4', 'OBJECTID'];
const COLORS = {SEQUENTIAL:'#68625e', UUIDV1:'#b42348', UUIDV7:'#047857', ULID:'#7534af', ULID_MONOTONIC:'#9854ba', UUIDV4:'#1d4ed8', OBJECTID:'#a16207'};
const LABELS = {SEQUENTIAL:'Sequential', UUIDV1:'UUIDv1', UUIDV7:'UUIDv7', ULID:'ULID', ULID_MONOTONIC:'ULID-mono', UUIDV4:'UUIDv4', OBJECTID:'ObjectId'};
const DBS = {postgres:'PostgreSQL', mysql:'MySQL', mongodb:'MongoDB', cassandra:'Cassandra'};
const SCALES = ['100k', '500k', '1m', '10m', '50m'];
const DEFAULT = {view:'summary', experiment:'A1', db:'cassandra', scale:'50m', metric:'throughput', mode:'keys', reference:'absolute', clusterMetric:'throughput', pgMetric:'page_splits', matrixDb:'postgres'};
let data;
let state = {...DEFAULT};
const content = document.querySelector('#content');
const esc = value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c]));
const number = (value, digits = 2) => Number(value).toLocaleString('en-US', {maximumFractionDigits:digits});
const signed = value => `${value > 0 ? '+' : ''}${number(value)}`;
const unique = items => [...new Set(items)];
const experiment = () => data.experiments.find(e => e.id === state.experiment);
const series = (exp, db, scale, metric) => data.entries.filter(e => e.experiment === exp && (!db || e.database === db) && (!scale || e.scale === scale) && (!metric || e.metric === metric)).sort((a,b) => KEY_ORDER.indexOf(a.keyType)-KEY_ORDER.indexOf(b.keyType));
const medianFor = (exp, db, scale, metric, key) => series(exp, db, scale, metric).find(e => e.keyType === key)?.median;
const ratio = (exp, metric='throughput') => medianFor(exp,'cassandra','50m',metric,'UUIDV7') / medianFor(exp,'cassandra','50m',metric,'UUIDV4');
const axisNumber = n => n >= 1000000 ? `${number(n/1000000,1)}M` : n >= 1000 ? `${number(n/1000,1)}k` : number(n, n < 1 ? 2 : 1);

function url(patch = {}) {
  const next = {...state, ...patch};
  const params = new URLSearchParams();
  const fields = next.view === 'summary' ? ['view','clusterMetric','pgMetric','matrixDb'] : ['view','experiment','db','scale','metric','mode','reference'];
  for (const field of fields) params.set(field, next[field]);
  return '#' + params.toString();
}
function link(text, patch, cls='text-link') { return `<a class="${cls}" href="${esc(url(patch))}">${esc(text)}</a>`; }
function field(label, key, options, value=state[key]) {
  return `<label class="field">${esc(label)}<select data-state="${key}" id="select-${key}">${options.map(([v,l]) => `<option value="${esc(v)}"${v === value ? ' selected' : ''}>${esc(l)}</option>`).join('')}</select></label>`;
}
function legend() {
  return `<div class="legend" aria-label="Key scheme legend">${KEY_ORDER.map(k => `<span><i aria-hidden="true" class="swatch${k==='UUIDV4'?' diamond':''}" style="--key:${COLORS[k]}"></i>${LABELS[k]}</span>`).join('')}</div>`;
}
function plotKey() { return '<div class="plot-key"><span><i class="sample-dot" aria-hidden="true"></i>Individual run</span><span><i class="sample-tick" aria-hidden="true"></i>Median</span></div>'; }

function normalize(entries, enabled) {
  if (!enabled) return entries;
  const baseline = entries.find(e => e.keyType === 'SEQUENTIAL')?.median;
  if (!(baseline > 0)) return [];
  return entries.map(e => ({...e, values:e.values.map(v => v / baseline * 100), median:e.median / baseline * 100}));
}
function niceMax(max) {
  if (!(max > 0)) return 1;
  const step = Math.pow(10, Math.floor(Math.log10(max)));
  return Math.ceil(max / step * 5) * step / 5;
}
function dotplot(entries, unit, {normalized=false, max=null, label='Individual measurements'} = {}) {
  if (!entries.length) return '<p class="empty">No measurements for this configuration.</p>';
  const width = 440, left = 94, right = 70, top = 24, row = 36;
  const bottom = top + row * entries.length;
  const height = bottom + 43;
  const upper = max || niceMax(Math.max(...entries.flatMap(e => e.values), normalized ? 100 : 0) * 1.07);
  const x = value => left + (width-left-right) * value / upper;
  const desc = `${label}. ${unit}. ${entries.map(e => `${LABELS[e.keyType]}: median ${number(e.median)}, ${e.values.length} runs`).join('; ')}. Dots are individual runs and vertical ticks mark medians. Exact values are available in the data table.`;
  let svg = `<svg class="plot-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(desc)}"><title>${esc(label)}</title>`;
  for (let i=0; i<=4; i++) {
    const value = upper * i / 4;
    svg += `<line class="guide" x1="${x(value)}" x2="${x(value)}" y1="10" y2="${bottom-12}"/><text x="${x(value)}" y="${bottom+10}" text-anchor="middle">${axisNumber(value)}</text>`;
  }
  if (normalized) svg += `<line class="baseline" x1="${x(100)}" x2="${x(100)}" y1="8" y2="${bottom-12}"/>`;
  svg += `<text x="${width-2}" y="11" text-anchor="end">median</text>`;
  entries.forEach((entry, index) => {
    const y = top + index * row;
    svg += `<text class="key-label" x="0" y="${y+4}">${LABELS[entry.keyType]}</text>`;
    entry.values.forEach((value, i) => {
      // Fixed vertical offsets expose coincident runs; no x jitter alters values.
      const cy = y + (i - (entry.values.length-1)/2) * 3.1;
      const title = `${LABELS[entry.keyType]} · run ${entry.runIds[i]}: ${value} ${unit}`;
      svg += entry.keyType === 'UUIDV4'
        ? `<path class="v4-mark" d="M ${x(value)} ${cy-4.4} l 4.4 4.4 -4.4 4.4 -4.4 -4.4 Z" fill="${COLORS[entry.keyType]}"><title>${esc(title)}</title></path>`
        : `<circle cx="${x(value)}" cy="${cy}" r="3.8" fill="${COLORS[entry.keyType]}"><title>${esc(title)}</title></circle>`;
    });
    svg += `<line class="median-mark" x1="${x(entry.median)}" x2="${x(entry.median)}" y1="${y-12}" y2="${y+12}"/><text class="median-label" x="${width-2}" y="${y+4}" text-anchor="end">${axisNumber(entry.median)}</text>`;
  });
  return svg + `<text x="${left}" y="${height-3}">${esc(unit)}${normalized ? ' · Sequential median = 100' : ''}</text></svg>`;
}
function panel({title, meta, exp, db='cassandra', scale='50m', metric='throughput', normalized=false, max=null, caption='', showLink=true}) {
  const entries = normalize(series(exp,db,scale,metric), normalized);
  const unit = normalized ? '%' : data.metrics[metric].unit;
  return `<figure class="plot-panel"><h3>${esc(title)}</h3><p class="plot-meta">${esc(meta)}</p>${dotplot(entries,unit,{normalized,max,label:`${title}, ${data.metrics[metric].label}`})}<figcaption class="chart-caption">${esc(caption)}</figcaption>${showLink ? link('Inspect runs & methods', {view:'explorer',experiment:exp,db,scale,metric,mode:'keys',reference:normalized?'sequential':'absolute'},'plot-link') : ''}</figure>`;
}

function miniComparison(patch) {
  const keys = patch.experiment === 'A1' || patch.metric === 'fragmentation' ? ['UUIDV7','UUIDV4'] : ['SEQUENTIAL','UUIDV4'];
  const entries = keys.map(key => series(patch.experiment,patch.db,patch.scale,patch.metric).find(e=>e.keyType===key));
  const max = Math.max(...entries.map(e=>e.median));
  const unit = data.metrics[patch.metric].unit;
  return `<svg class="mini-comparison" viewBox="0 0 240 58" role="img" aria-label="${esc(entries.map(e=>`${LABELS[e.keyType]}: ${number(e.median)} ${unit}`).join('; '))}">${entries.map((e,i)=>`<text x="0" y="${14+i*23}">${e.keyType==='SEQUENTIAL'?'Seq.':LABELS[e.keyType]}</text><rect x="76" y="${5+i*23}" width="${max>0?e.median/max*108:0}" height="10" fill="${COLORS[e.keyType]}"/><text x="240" y="${14+i*23}" text-anchor="end">${axisNumber(e.median)}</text>`).join('')}<text x="240" y="57" text-anchor="end">${esc(unit)}</text></svg>`;
}
function summary() {
  const target = (experiment, db, scale, metric='throughput') => ({view:'explorer',experiment,db,scale,metric,mode:'keys',reference:'absolute'});
  const pgPenalty = (1 - medianFor('single-insert','postgres','1m','throughput','UUIDV4') / medianFor('single-insert','postgres','1m','throughput','SEQUENTIAL')) * 100;
  const fragmentation = medianFor('single-insert','postgres','1m','fragmentation','UUIDV4');
  const mysqlPenalty = (1 - medianFor('single-insert','mysql','10m','throughput','UUIDV4') / medianFor('single-insert','mysql','10m','throughput','SEQUENTIAL')) * 100;
  const findings = [
    ['PostgreSQL inserts', `−${number(pgPenalty,1)}%`, 'UUIDv4 throughput vs. Sequential', '1M rows · 1 client · n=5', target('single-insert','postgres','1m')],
    ['Leaf fragmentation', `${number(fragmentation,1)}%`, 'PostgreSQL · UUIDv4 index', '1M rows · n=5', target('single-insert','postgres','1m','fragmentation')],
    ['MySQL inserts', `−${number(mysqlPenalty,1)}%`, 'UUIDv4 throughput vs. Sequential', '10M rows · 1 client · n=5', target('single-insert','mysql','10m')],
    ['Cassandra reads', `${number(ratio('A1'))}×`, 'UUIDv7 / UUIDv4 attempted throughput', '50M rows · 4 GB · 3 nodes / RF3 · n=5', target('A1','cassandra','50m')],
  ];
  const architectures = {postgres:'B-tree / heap-organized',mysql:'Clustered B-tree (InnoDB)',mongodb:'WiredTiger B-tree index',cassandra:'LSM-tree / SSTables'};
  return `<section class="summary-meta"><div class="wrap">Medians · 5 runs per configuration (A4: 3) · Separate single-node and cluster experiments ${link('Methods', {view:'data'}, 'inline-link')}</div></section>
  <section class="wrap compact-section"><h1>Key findings</h1><div class="finding-grid">${findings.map(([title,value,description,meta,patch])=>`<a class="finding" href="${esc(url(patch))}"><h2>${title}</h2><strong>${value}</strong><p>${description}</p><small>${meta}</small>${miniComparison(patch)}</a>`).join('')}</div></section>
  <section class="database-band"><div class="wrap compact-section"><h2>Databases tested</h2><div class="database-grid">${Object.entries(DBS).map(([db,label])=>`<a class="database-entry" data-db="${db}" href="${esc(url(target('single-insert',db,'1m')))}"><h3>${label}</h3><p>${architectures[db]}</p><span>Explore</span></a>`).join('')}</div></div></section>
  <section class="wrap compact-section key-section"><h2>Key types tested</h2>${legend()}</section>
  <details class="additional-results"><summary class="wrap">More results: Cassandra memory budgets, PostgreSQL indexes, workload comparisons</summary>${detailedFindings()}</details>`;
}
function detailedFindings() {
  const cm = state.clusterMetric;
  const contrastA5 = data.contrasts.A5.meanThroughput;
  const contrastA2 = data.contrasts.A2.meanThroughput;
  const storageExcess = (medianFor('A5','cassandra','50m','table_size_mb','UUIDV4') / medianFor('A5','cassandra','50m','table_size_mb','SEQUENTIAL') - 1) * 100;
  const pm = state.pgMetric;
  return `<section class="section"><div class="wrap"><div class="section-heading"><div><h2>Cassandra · 50M rows</h2><p>Cassandra at 50M rows: compare key schemes <em>within</em> each experiment. The panels use independent axes.</p></div><span class="section-reference">§6 · Cluster results</span></div><div class="section-tools">${field('Measurement','clusterMetric',[['throughput','Throughput'],['table_size_mb','Table size'],['read_iops','Block-read rate'],['write_iops','Block-write rate']])}${plotKey()}</div><div class="plot-grid">
  ${panel({title:'Inserts · 4 GB',meta:'A5 · 3 nodes / RF3 · 8 writers · n=5',exp:'A5',metric:cm,caption:cm==='throughput'?`Similar observed medians. v4/v7 relative mean difference: ${signed(contrastA5.difference)}%; 95% CI [${signed(contrastA5.ci[0])}, ${signed(contrastA5.ci[1])}] (combined-mean denominator).`:cm==='table_size_mb'?`UUIDv4 table-size median: ${number(storageExcess,1)}% above Sequential. Repeated, highly compressible payload.`:data.metrics[cm].note})}
  ${panel({title:'Reads · 4 GB',meta:'A1 · 3 nodes / RF3 · 1 reader · n=5',exp:'A1',metric:cm,caption:cm==='throughput'?`UUIDv7 reaches ${number(ratio('A1'))}× the UUIDv4 median. Every v4 run is below every v7 run.`:data.metrics[cm].note})}
  ${panel({title:'Reads · 32 GB',meta:'A2 · 3 nodes / RF3 · 1 reader · n=5',exp:'A2',metric:cm,caption:cm==='throughput'?'No statistically significant v4/v7 throughput difference; this does not establish equivalence.':data.metrics[cm].note})}
  </div><div class="result-note"><div><p>HDD storage. Reads immediately after loading, without waiting for compaction. The 4/32 GB contrast changes container memory, heap and new generation together.</p>${link('Single-node check: A3', {view:'explorer',experiment:'A3',db:'cassandra',scale:'50m',metric:'throughput',mode:'keys',reference:'absolute'})} &nbsp; ${link('Target-selection check: A4', {view:'explorer',experiment:'A4',db:'cassandra',scale:'50m',metric:'throughput',mode:'keys',reference:'absolute'})}</div><p class="muted">At 32 GB, the v4/v7 relative mean-throughput difference has a 95% interval of [${signed(contrastA2.ci[0])}%, ${signed(contrastA2.ci[1])}%]. The denominator is the two groups’ combined mean. Substantial differences in both directions remain compatible with this estimate.</p></div></div></section>
  <section class="section shaded"><div class="wrap"><div class="section-heading"><div><h2>PostgreSQL index structure</h2><p>PostgreSQL: split counts, leaf occupancy and index size tell different parts of the story.</p></div><span class="section-reference">§5.2 · Figure 2</span></div><div class="section-tools">${field('Index measurement','pgMetric',['page_splits','avg_leaf_density','index_size_mb','fragmentation'].map(m=>[m,data.metrics[m].label]))}${plotKey()}</div><div class="plot-grid two">${['1m','10m'].map(scale=>panel({title:`${scale.toUpperCase()} rows`,meta:'PostgreSQL · inserts · 1 client · n=5',exp:'single-insert',db:'postgres',scale,metric:pm,caption:data.metrics[pm].note})).join('')}</div><div class="result-note"><p><strong>UUIDv1 runs separate into two observed ranges at 10M</strong> in split count and leaf density. The timestamp layout is a candidate explanation; individual wrap events were not recorded.</p><p class="muted">Do not read fragmentation as wasted-space percentage. Both ULID variants can have split counts close to UUIDv4’s while retaining higher leaf density. Axes are independent; values are absolute.</p></div></div></section>
  <section class="section"><div class="wrap"><div class="section-heading"><div><h2>Throughput by workload</h2><p>Within-engine throughput, relative to each workload’s own Sequential median. Select a value to inspect its evidence.</p></div><span class="section-reference">§5 · Single-node results</span></div><div class="section-tools">${field('Database','matrixDb',Object.entries(DBS))}</div>${workloadMatrix()}<p class="matrix-note">100% = the Sequential median in that column. Different preload sizes, target selection and implementations are part of each workload. This is not an isolated operation-mix effect. IH uses only the corrected, selected 125-run dataset.</p>${legend()}</div></section>`;
}
function workloadMatrix() {
  const cols = [['single-insert','1m','Inserts','1M rows'],['single-read','1m','Reads','1M rows'],['single-update','1m','Updates','1M rows'],['single-ru','500k','Read / update','500K preload'],['single-ih','100k','Insert-heavy','100K preload']];
  const keys = KEY_ORDER.filter(k=>k!=='OBJECTID'||state.matrixDb==='mongodb');
  return `<div class="matrix-scroll" role="region" aria-label="Workload throughput comparisons" tabindex="0"><table class="matrix"><caption>${DBS[state.matrixDb]} · median throughput as % of the workload-specific Sequential median · n=5</caption><thead><tr><th scope="col">Key scheme</th>${cols.map(c=>`<th scope="col" class="numeric">${c[2]}<small>${c[3]}</small></th>`).join('')}</tr></thead><tbody>${keys.map(key=>`<tr><th scope="row">${LABELS[key]}</th>${cols.map(([exp,scale])=>{
    const value = medianFor(exp,state.matrixDb,scale,'throughput',key)/medianFor(exp,state.matrixDb,scale,'throughput','SEQUENTIAL')*100;
    const shade = Math.min(.13,Math.abs(100-value)/350);
    return `<td style="--cell-wash:rgba(29,78,216,${shade})"><a aria-label="${esc(`${LABELS[key]}, ${data.experiments.find(e=>e.id===exp).label}: ${number(value,1)} percent. Inspect configuration`)}" href="${esc(url({view:'explorer',experiment:exp,db:state.matrixDb,scale,metric:'throughput',mode:'keys',reference:'sequential'}))}">${number(value,1)}%</a></td>`;
  }).join('')}</tr>`).join('')}</tbody></table></div>`;
}

function available() {
  const entries = series(state.experiment);
  const dbs = unique(entries.map(e=>e.database));
  const scales = unique(entries.filter(e=>e.database===state.db).map(e=>e.scale)).sort((a,b)=>SCALES.indexOf(a)-SCALES.indexOf(b));
  const metrics = unique(entries.filter(e=>e.database===state.db&&e.scale===state.scale).map(e=>e.metric));
  const modes = [['keys','Key schemes']];
  if (scales.length>1) modes.push(['scales','Dataset sizes']);
  if (dbs.length>1) modes.push(['engines','Engines · normalized']);
  return {dbs,scales,metrics,modes};
}
function validateState() {
  if (!['summary','explorer','data'].includes(state.view)) state.view='summary';
  if (!data.experiments.some(e=>e.id===state.experiment)) state.experiment='A1';
  let opts = available();
  if (!opts.dbs.includes(state.db)) state.db=opts.dbs[0];
  opts=available();
  if (!opts.scales.includes(state.scale)) state.scale=opts.scales.includes('1m')?'1m':opts.scales[0];
  opts=available();
  if (!opts.metrics.includes(state.metric)) state.metric='throughput';
  if (!opts.modes.some(([m])=>m===state.mode)) state.mode='keys';
  // Only throughput is compared across engines; no structural proxy comparisons.
  if (state.mode==='engines') { state.metric='throughput'; state.reference='sequential'; }
  if (state.metric!=='throughput'||!['absolute','sequential'].includes(state.reference)) state.reference='absolute';
  if (!['throughput','table_size_mb','read_iops','write_iops'].includes(state.clusterMetric)) state.clusterMetric='throughput';
  if (!['page_splits','avg_leaf_density','index_size_mb','fragmentation'].includes(state.pgMetric)) state.pgMetric='page_splits';
  if (!DBS[state.matrixDb]) state.matrixDb='postgres';
}
function filters() {
  const opts = available();
  return `<section class="filters-band" aria-label="Evidence filters"><div class="wrap"><div class="filters">${field('Experiment', 'experiment', data.experiments.map(e=>[e.id,e.label]))}${field('Database','db',opts.dbs.map(db=>[db,DBS[db]]))}${field('Rows / preload','scale',opts.scales.map(s=>[s,s.toUpperCase()]))}${field('Measurement','metric',(state.mode==='engines'?['throughput']:opts.metrics).map(m=>[m,data.metrics[m].label]))}</div><div class="filter-secondary">${field('Compare','mode',opts.modes)}${state.metric==='throughput'&&state.mode!=='engines'?field('Reference','reference',[['absolute','Absolute values'],['sequential','Sequential median = 100%']]):''}</div></div></section>`;
}
function context() {
  const e = experiment();
  return `<section class="wrap context" aria-label="Experimental conditions"><div class="context-line"><span>${esc(e.section)}</span><span>${esc(e.family==='cluster' ? e.rows+' rows' : ['single-ih','single-ru'].includes(e.id) ? state.scale.toUpperCase()+' preload rows' : state.mode==='scales' ? e.rows+' rows' : state.scale.toUpperCase()+' rows')}</span><span>${e.memory} / container</span><span>${e.nodes} node${e.nodes===1?'':'s'}${e.rf?' / RF'+e.rf:''}</span><span>${e.clients} ${e.clients===1?'client':'writers'}</span><span>n=${e.n} / scheme</span></div><details class="condition-details"><summary>Conditions &amp; sampling</summary><p>${esc(e.note)}</p>${e.family==='cluster'?`<p class="small">Heap / new generation: ${esc(e.heap)} · 8 CPUs per container · 50,000 buckets · LOCAL_ONE · ${esc(e.sampler)}.</p>`:`<p class="small">Ryzen 7 7840U workstation · NVMe · 4 CPUs per container · ${esc(e.sampler)}. Host throughput is not compared with the HDD cluster campaign.</p>`}</details></section>`;
}
function currentGroups() {
  if (state.mode==='engines') return Object.keys(DBS).filter(db=>series(state.experiment,db,state.scale,state.metric).length).map(db=>({db,scale:state.scale,title:DBS[db]}));
  if (state.mode==='scales') return available().scales.filter(scale=>series(state.experiment,state.db,scale,state.metric).length).map(scale=>({db:state.db,scale,title:scale.toUpperCase()+' rows'}));
  return [{db:state.db,scale:state.scale,title:DBS[state.db]}];
}
function explorerCharts() {
  const groups=currentGroups();
  const normalized=state.reference==='sequential';
  const max=niceMax(Math.max(...groups.flatMap(g=>normalize(series(state.experiment,g.db,g.scale,state.metric),normalized).flatMap(e=>e.values)),normalized?100:0)*1.07);
  const plots=groups.map(g=>panel({title:g.title,meta:`${g.scale.toUpperCase()} ${experiment().id==='single-ih'||experiment().id==='single-ru'?'preload':'rows'} · n=${experiment().n} · ${data.metrics[state.metric].label}`,exp:state.experiment,db:g.db,scale:g.scale,metric:state.metric,normalized,max,showLink:false,caption:normalized?'Each run divided by the Sequential median in its own configuration. This is not a paired-run ratio.':'Points: individual runs. Ticks: medians. Values are descriptive unless a contrast is reported below.'})).join('');
  const stats=contrast();
  const basis=state.metric==='throughput' ? (state.experiment==='single-ih'?'Successful operations / s':'Attempted operations / s') : data.metrics[state.metric].unit;
  return `<section class="wrap explorer-chart"><div class="section-heading"><div><h2>${esc(data.metrics[state.metric].label)}</h2><p>${esc(basis)}${normalized?' · Sequential median = 100%':''}${groups.length>1?' · Shared axis':''}${state.metric==='throughput'&&['A1','A2','A3','A4'].includes(state.experiment)?' · Includes no-row responses':''}</p></div>${plotKey()}<button class="copy-chart" type="button" data-action="copy">Copy link</button></div><div class="chart-workspace${stats?' has-contrast':''}"><div class="chart-panels">${groups.length>1?`<div class="plot-grid ${groups.length===4?'four':groups.length===2?'two':''}">${plots}</div>`:plots}</div>${stats}</div><details class="measurement-details"><summary>Measurement definition${state.experiment==='A2'?' · I/O exclusion':''}</summary><p class="metric-description">${esc(data.metrics[state.metric].note)}${normalized?' Each run is divided by the Sequential median in its own configuration; ratios are not paired.':''}</p></details></section>`;
}
function contrast() {
  const exp=state.experiment;
  if (exp==='A4') return '<div class="contrast"><div><h3>Method check, not a headline ranking</h3></div><p>A4 changes both the target set and read order. Its n=3 results are descriptive. They are not pooled with A1–A3 or used to claim an isolated sampler effect.</p></div>';
  const c=data.contrasts[exp]?.[state.metric];
  const mean=data.contrasts[exp]?.meanThroughput;
  if (!c && !(mean&&state.metric==='throughput')) return '';
  let primary=c?`<div><h3>UUIDv4 / UUIDv7</h3><div class="contrast-values">${number(c.ratio,3)} <span class="small">ratio of medians</span></div><p>95% bootstrap interval [${number(c.ci[0],3)}, ${number(c.ci[1],3)}].<br>Exact ${c.sided} rank-sum p = ${number(c.p,5)}.</p></div>`:`<div><h3>UUIDv4 versus UUIDv7</h3><div class="contrast-values">${signed(mean.difference)}%</div><p>Relative difference of mean throughput.<br>95% Welch interval [${signed(mean.ci[0])}%, ${signed(mean.ci[1])}%].</p></div>`;
  const secondary=c&&state.metric!=='throughput';
  return `<aside class="contrast" aria-label="Paper statistical contrast">${primary}${exp==='A2'?'<p>No significant throughput difference; equivalence is not established.</p>':''}<details><summary>Statistical method</summary><p>${c?`For ${state.metric==='throughput'?'throughput, below 1':'latency and I/O, above 1'} means UUIDv4 is worse. 10,000 percentile-bootstrap resamples (seed 20260905). Intervals describe the ratio of medians, not the spread of individual runs.`:'The difference is divided by the two groups’ combined observed mean, not the UUIDv7 mean. The interval spans both a deficit and a small advantage for UUIDv4; similar observed medians do not establish equivalence.'}</p><p>${secondary?'Supporting endpoint: the four supporting endpoints use a Bonferroni threshold of 0.0125. These related measurements are not independent confirmations.':'Throughput is the primary endpoint. Small sample sizes limit precision; no equivalence conclusion is established.'}</p>${exp==='A2'&&state.metric==='throughput'?`<p>Relative mean-throughput difference: ${signed(mean.difference)}%, Welch 95% interval [${signed(mean.ci[0])}%, ${signed(mean.ci[1])}%], divided by the combined mean. No significant difference is not equivalence. A mean-difference interval and a median-ratio interval estimate different quantities.</p>`:''}</details></aside>`;
}
function selectedEntries() {
  return currentGroups().flatMap(g=>series(state.experiment,g.db,g.scale,state.metric));
}
function table(open=false) {
  const rows=selectedEntries();
  const n=experiment().n;
  return `<section class="wrap details-table"><details${open?' open':''}><summary>Individual measurements · ${rows.length} series · ${data.metrics[state.metric].unit} (absolute)</summary><div class="table-actions"><p>Values displayed to 6 decimals; CSV retains source precision. Run numbers identify repetitions, not paired comparisons.</p><button type="button" data-action="download">Download CSV</button>${state.view==='data'?`<a href="data/evidence.json" download>Complete dataset · JSON</a>${link('Open chart',{view:'explorer'},'inline-link')}<button type="button" data-action="copy">Copy link</button>`:''}</div><div class="table-scroll" role="region" aria-label="Individual run values" tabindex="0"><table><caption>${esc(experiment().label)} · ${esc(data.metrics[state.metric].label)}. Each source link opens the original measurement record.</caption><thead><tr><th scope="col">Configuration / key</th>${Array.from({length:n},(_,i)=>`<th scope="col" class="numeric">Run ${i+1}</th>`).join('')}<th scope="col" class="numeric">Median</th><th scope="col">Source</th></tr></thead><tbody>${rows.map(e=>`<tr><th scope="row">${DBS[e.database]} · ${e.scale.toUpperCase()}${state.view==='data'?' · ':'<br>'}${LABELS[e.keyType]}</th>${e.values.map((v,i)=>`<td class="run-values" title="${esc(e.runIds[i])}">${number(v,6)}</td>`).join('')}<td class="run-values">${number(e.median,6)}</td><td><a href="${esc(data.sources[e.source].path)}" download>Source</a></td></tr>`).join('')}</tbody></table></div>${state.experiment==='single-ih'?'<p class="matrix-note">Run 1–5 are blocks 1–5. Selected replacements: PostgreSQL block 5 UUIDv4 and ULID-mono; MySQL block 1 ULID. Exact logical run IDs are included in the selection CSV download and source selection.json.</p>':''}</details></section>`;
}
function methods() {
  return `<section class="section"><div class="wrap"><div class="section-heading"><h2>Methods &amp; limitations</h2><span class="section-reference">§4 · Methods &amp; §7 · Limitations</span></div><div class="method-layout">
  <section><h3>Two deployments, separate evidence</h3><p>Single-node workloads use a Ryzen 7 7840U workstation, NVMe storage and containers configured for 4 CPUs / 8 GB. The corrected IH campaign uses a newer kernel than the historical workloads.</p><p>The 50M campaign uses Xeon Gold 6326 hosts and SATA HDD storage, with the driver on a separate host. Each Cassandra container has 8 CPUs. A1/A2/A4/A5 use three nodes and RF3; A3 uses one node and RF1. Do not infer a scaling curve or isolated replication effect across these deployments.</p></section>
  <section><h3>What a point and an interval mean</h3><p>One dot is one run; a vertical tick is the median of that scheme’s runs. Vertical offsets reveal overlapping values and do not encode another variable. The dot spread is not a confidence interval.</p><p>For A1–A3, the primary contrast is UUIDv4 / UUIDv7 throughput. Ratio intervals use 10,000 percentile-bootstrap resamples with seed 20260905. Exact rank-sum tests are one-sided in A1/A3, two-sided in A2. Four supporting endpoints use a 0.0125 Bonferroni threshold. No significant difference does not prove equivalence.</p></section>
  <section><h3>Reads, sampling and measurement windows</h3><p>A1–A3 record identifiers at uniformly sampled insertion positions during loading, then shuffle them. A4 fetches the two smallest clustering keys per partition and retains returned order. In historical single-node workloads, PostgreSQL draws random targets; MySQL, MongoDB and Cassandra fetch engine-specific limited lists.</p><p>Outside corrected IH, throughput and latency use the timed workload loop. cgroup I/O covers broader process windows, including target fetching where applicable and concurrent background work. Reads start after loading without waiting for compaction. No claim of steady-state performance is made.</p><p>I/O per lookup is excluded when both the arm-wide median read I/O exceeds 1 block operation/s and the median per-run write/read I/O ratio exceeds 5%. A2 meets both conditions (${number(data.ioContexts.A2.medianReadIops)} block ops/s; ${number(data.ioContexts.A2.medianWriteReadPct)}%), despite near-zero read I/O compared with A1. Insert-side counters end when the workload returns; later flushes and compactions are missed by an unknown amount.</p></section>
  <section><h3>Representations and structural measurements</h3><p>Cassandra uses <code>PRIMARY KEY ((bucket), id)</code>: id is the clustering key, not the hashed partition key. Part one fixes bucket=1; the 50M campaign hashes IDs into 50,000 buckets. UUIDv4/v7 share CQL <code>uuid</code>; UUIDv1 uses <code>timeuuid</code>, ULIDs <code>blob</code> and Sequential <code>bigint</code>.</p><p>PostgreSQL ULIDs use the extension’s <code>ulid</code> type; UUID/ULID storage and generators differ across engines. Split records, leaf density and fragmentation are distinct. SSTable overlap and key compression remain hypotheses. Bloom false-positive counters do not identify the mechanism.</p></section>
  <section><h3>Corrected insert-heavy selection</h3><p>The selected protocol is <code>ih-fixed-preload-v1</code>: 100K verified preload rows, a fixed pool of all preload IDs, and 200K operations with 70% insert / 30% read probabilities. All selected operations succeed and all reads hit. Throughput counts completed successful operations divided by elapsed mixed-phase duration.</p><p>For PostgreSQL IH, that duration includes <code>pgbench</code> startup, connection establishment and full transaction logging. Preload, target retrieval and validation lie outside the window; pre-run validation can warm caches. This denominator differs from the timed workload loop used by the other workloads.</p><p>125 selected runs comprise 122 originals and three repeats after documented overlaps with paper-build/render activity. Repeats replace, rather than augment, their logical slots. Historical IH data, failed consolidations, smoke tests and superseded originals are not selected. This does not prove an entirely interference-free host.</p></section>
  <section><h3>Scope of the published measurements</h3><p>These comparisons include each implementation, representation, sampler and workload. Every load reuses a 1 KiB payload, so storage results are specific to highly compressible data. A5 table size sums all three replicas.</p><p>This explorer selects the paper’s one-client historical configurations, corrected IH, and A1–A5. Older concurrent runs and June cluster pilots remain in the repository, not in these charts. Some downloaded historical source CSVs contain additional scenarios: only the explicitly selected rows appear here.</p></section>
  </div></div></section>`;
}
function sourceList(names) {
  return `<ul class="source-list">${names.map(name=>{const s=data.sources[name];return `<li><a href="${esc(s.path)}" download>${esc(name)}</a><span class="source-hash">SHA-256 ${s.sha256} · ${number(s.bytes,0)} bytes${s.note?' · '+esc(s.note):''}</span></li>`;}).join('')}</ul>`;
}
function sources() {
  let names=unique(series(state.experiment).map(e=>e.source));
  if (state.experiment==='single-ih') names.push(...Object.keys(data.sources).filter(n=>n.startsWith('ih/')&&!names.includes(n)));
  if (experiment().family==='cluster') names.push(...names.map(n=>n.replace('.runs.jsonl','.meta.json')).filter(n=>data.sources[n]));
  const analysis=Object.keys(data.sources).filter(n=>n.startsWith('analysis/'));
  return `<section class="section shaded"><div class="wrap"><div class="section-heading"><div><h2>Source files</h2><p>Downloadable source snapshots, with hashes. The chart uses the selected rows and full-precision cluster run logs.</p></div></div><div class="method-layout"><section><h3>Sources · ${esc(state.experiment)}</h3>${sourceList(names)}</section><section><h3>Analysis &amp; complete dataset</h3><p><a href="data/evidence.json" download>Download all selected evidence · JSON</a></p><p>The build runs the paper’s validators, recomputes contrasts with the paper’s analysis functions and checks numerical agreement with its generated macros. The IH gate also checks archived per-run evidence locally; this site hosts selection records, not the large transaction-log bundle.</p><details><summary>Analysis scripts &amp; paper-number snapshots</summary>${sourceList(analysis)}</details><p><a href="EVIDENCE.md">Build instructions and dataset contract</a></p><p class="small">This is an artifact snapshot, not a live benchmark. Paper section references follow the working paper. No publication DOI or final paper URL is assumed.</p></section></div><div class="provenance-strip"><span>${esc(data.snapshot)}</span><span>Source manifest <code>${data.sourceDigest.slice(0,16)}</code></span></div></div></section>`;
}
function experimentMatrix() {
  return `<section class="section"><div class="wrap"><h2>Experiment register</h2><div class="table-scroll" role="region" aria-label="Experiment register" tabindex="0"><table class="experiment-table"><caption>Each experiment has its own baseline. The 50M follow-up is never appended to the single-node scale series.</caption><thead><tr><th scope="col">Experiment</th><th scope="col">Rows / preload</th><th scope="col">Memory / node</th><th scope="col">Nodes / RF</th><th scope="col">Clients</th><th scope="col">Runs / scheme</th></tr></thead><tbody>${data.experiments.map(e=>`<tr><th scope="row">${link(e.label,{view:'explorer',experiment:e.id,metric:'throughput',mode:'keys',reference:'absolute'},'register-link')}</th><td>${esc(e.rows)}</td><td>${e.memory}</td><td>${e.nodes}${e.rf?' / '+e.rf:''}</td><td>${e.clients}</td><td>${e.n}</td></tr>`).join('')}</tbody></table></div></div></section>`;
}
function render({focusControl=null, focusMain=false}={}) {
  const expanded = content.querySelector('.additional-results')?.open || ['clusterMetric','pgMetric','matrixDb'].some(key => state[key] !== DEFAULT[key]);
  validateState();
  content.dataset.view=state.view;
  document.querySelectorAll('[data-view]').forEach(a=>{
    a.href=url({view:a.dataset.view});
    if (a.dataset.view===state.view) a.setAttribute('aria-current','page'); else a.removeAttribute('aria-current');
  });
  if (state.view==='summary') content.innerHTML=summary();
  else if (state.view==='explorer') content.innerHTML=`<h1 class="sr-only">Explorer</h1>${filters()}${context()}${explorerCharts()}${table()}`;
  else content.innerHTML=`<h1 class="sr-only">Data &amp; methods</h1>${filters()}${context()}${table(true)}<div class="data-disclosures"><details><summary class="wrap">Measurement definition · ${esc(data.metrics[state.metric].label)}</summary><p class="wrap data-definition">${esc(data.metrics[state.metric].note)}</p></details><details><summary class="wrap">Source files &amp; provenance</summary>${sources()}</details><details><summary class="wrap">Experiment register · ${data.experiments.length} experiments</summary>${experimentMatrix()}</details><details><summary class="wrap">Methods &amp; limitations</summary>${methods()}</details></div>`;
  document.title=`UUID Benchmark — ${state.view==='summary'?'Paper evidence':state.view==='explorer'?experiment().label:'Data & methods'}`;
  if (state.view==='summary' && expanded) content.querySelector('.additional-results').open=true;
  // Canonical URLs discard invalid combinations without creating a history loop.
  history.replaceState(null,'',url());
  if (focusControl) document.getElementById(`select-${focusControl}`)?.focus();
  if (focusMain) document.querySelector('#main').focus({preventScroll:true});
}
function readURL() {
  const p=new URLSearchParams(location.hash.slice(1));
  state={...DEFAULT};
  for (const key of Object.keys(DEFAULT)) if(p.has(key)) state[key]=p.get(key);
  // Preserve useful old dashboard links without importing its old interpretations.
  if (p.get('view')==='raw-data') state.view='data';
  if (!p.has('experiment')&&p.has('scenario')) state.experiment=({insert_performance:'single-insert',read_performance:'single-read',update_performance:'single-update',mixed_read_update:'single-ru',mixed_insert_heavy:'single-ih'})[p.get('scenario')]||'single-insert';
  if (p.get('mode')==='cross-db') state.mode='engines';
  if (p.get('mode')==='scale') state.mode='scales';
}
function announce(text) { document.querySelector('#announcement').textContent=text; }
function downloadCSV() {
  const header=['experiment','database','scale','metric','unit','key_type','run_id','value','source','source_sha256'];
  const rows=selectedEntries().flatMap(e=>e.values.map((value,i)=>[e.experiment,e.database,e.scale,e.metric,data.metrics[e.metric].unit,e.keyType,e.runIds[i],value,e.source,data.sources[e.source].sha256]));
  const csv=[header,...rows].map(row=>row.map(v=>`"${String(v).replaceAll('"','""')}"`).join(',')).join('\r\n');
  const blob=new Blob([csv],{type:'text/csv;charset=utf-8'});
  const href=URL.createObjectURL(blob), a=document.createElement('a');
  a.href=href;a.download=`uuid-${state.experiment}-${state.metric}-${state.mode}.csv`;a.click();
  setTimeout(()=>URL.revokeObjectURL(href),1000);
  announce('Selected individual measurements downloaded as CSV.');
}
content.addEventListener('change',event=>{
  const key=event.target.dataset.state;
  if (!key) return;
  state[key]=event.target.value;
  // Explicit experiment changes start with that experiment's primary endpoint.
  if (key==='experiment') {state.metric='throughput';state.mode='keys';state.reference='absolute';}
  if (key==='metric'&&state.metric!=='throughput') state.reference='absolute';
  validateState();
  history.pushState(null,'',url());
  render({focusControl:key});
  announce(`${state.view==='summary'?'Findings updated':`${experiment().label}, ${data.metrics[state.metric].label}`}.`);
});
content.addEventListener('click',async event=>{
  const button=event.target.closest('[data-action]');
  if (!button) return;
  if(button.dataset.action==='download') downloadCSV();
  if(button.dataset.action==='copy') {
    try {await navigator.clipboard.writeText(location.href);button.textContent='Link copied';announce('View link copied.');}
    catch {button.textContent='Copy the address bar URL';announce('Clipboard is unavailable. Copy the URL from your address bar.');}
  }
});
// The routing hash stores the selected evidence. Skip navigation must only move
// focus and scroll, never replace that state with the fragment "#main".
document.querySelector('.skip-link').addEventListener('click', event => {
  event.preventDefault();
  const main = document.querySelector('#main');
  main.focus({preventScroll:true});
  main.scrollIntoView({behavior:'instant', block:'start'});
});
window.addEventListener('hashchange',()=>{
  if (!data) return;
  const previousView=state.view;
  readURL();render({focusMain:previousView!==state.view});
  if(previousView!==state.view) window.scrollTo({top:0,behavior:'instant'});
});
async function start() {
  const loading=document.querySelector('#load-state');
  try {
    const response=await fetch('data/evidence.json');
    if(!response.ok) throw new Error(`HTTP ${response.status}`);
    data=await response.json();
    if(data.schemaVersion!==1||!data.entries?.length||!data.experiments?.length) throw new Error('Unsupported or empty evidence dataset');
    readURL();render();loading.hidden=true;
  } catch(error) {
    console.error('Evidence could not be loaded:',error);
    loading.innerHTML='<h1>Evidence could not be loaded.</h1><p>The dataset is unavailable or incompatible. Reload to try again, or download the source snapshot.</p><button type="button" id="retry">Try again</button> <a href="data/evidence.json">Open evidence JSON</a>';
    loading.setAttribute('role','alert');
    document.querySelector('#retry').addEventListener('click',()=>location.reload());
  }
}
start();
