"""Fast, dependency-free checks for the published evidence snapshot.
Run: python3 -m unittest discover -s scripts -p test_evidence.py
The full source-evidence gate runs in build_evidence.py before publishing files.
"""
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import statistics as st
import unittest
import math
import subprocess
import tempfile

from check_evidence_package import check_git_eligibility, stage_candidate, validate_artifact

DOCS = Path(__file__).resolve().parent.parent / 'docs'


class EvidenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = json.loads((DOCS/'data/evidence.json').read_text())
        cls.entries = cls.data['entries']
        cls.experiments = {e['id']: e for e in cls.data['experiments']}

    def test_identity_and_complete_run_groups(self):
        identities = set()
        for e in self.entries:
            identity = tuple(e[k] for k in ('experiment','database','scale','metric','keyType'))
            self.assertNotIn(identity, identities)
            identities.add(identity)
            self.assertEqual(len(e['values']), self.experiments[e['experiment']]['n'])
            self.assertEqual(len(set(e['runIds'])), len(e['values']))
            self.assertEqual(st.median(e['values']), e['median'])
            self.assertIn(e['metric'], self.data['metrics'])
        self.assertEqual(len(self.entries), 700)

    def test_all_source_hashes(self):
        for name, source in self.data['sources'].items():
            path = DOCS/source['path']
            self.assertTrue(path.resolve().is_relative_to(DOCS.resolve()))
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), source['sha256'], name)
            self.assertEqual(path.stat().st_size, source['bytes'])
        digest = hashlib.sha256(json.dumps(self.data['sources'], sort_keys=True).encode()).hexdigest()
        self.assertEqual(digest, self.data['sourceDigest'])

    def test_every_plotted_value_reconstructs_from_source(self):
        cache = {}
        for e in self.entries:
            name=e['source']
            if name not in cache:
                p=DOCS/self.data['sources'][name]['path']
                cache[name]=[json.loads(l) for l in p.read_text().splitlines()] if p.suffix=='.jsonl' else list(csv.DictReader(io.StringIO(p.read_text())))
            rows=cache[name]
            exp=e['experiment']
            if exp.startswith('A'):
                rows=sorted([r for r in rows if r['key_type'].upper()==e['keyType']],key=lambda r:r['run'])
                field='read_throughput' if e['metric']=='throughput' and exp!='A5' else e['metric']
                values=[r['metrics']['read_iops']/r['metrics']['read_throughput'] if field=='io_per_op' else r['metrics'][field] for r in rows]
            elif exp=='single-ih':
                rows=sorted([r for r in rows if r['engine']==e['database'] and r['scheme'].upper()==e['keyType']],key=lambda r:int(r['block']))
                field={'p50_latency_us':'latency_p50_us','p95_latency_us':'latency_p95_us','p99_latency_us':'latency_p99_us'}.get(e['metric'],e['metric'])
                values=[float(r[field]) for r in rows]
                self.assertEqual([r['run_id'] for r in rows],e['runIds'])
            else:
                scenario={'single-insert':'insert_performance','single-read':'read_performance','single-update':'update_performance','single-ru':'mixed_read_update'}[exp]
                metric={'single-read':'read_throughput','single-update':'update_throughput','single-ru':'overall_throughput'}.get(exp,'throughput') if e['metric']=='throughput' else e['metric']
                selected=[r for r in rows if r['Scenario']==scenario and r['Metric']==metric and r['KeyType']==e['keyType']]
                self.assertEqual(len(selected),1)
                values=[float(selected[0][f'Run{i}']) for i in range(1,6)]
            self.assertEqual(values,e['values'],(exp,e['database'],e['metric'],e['keyType']))

    def test_campaign_boundaries(self):
        self.assertEqual(set(self.experiments), {'single-insert','single-read','single-update','single-ru','single-ih','A1','A2','A3','A4','A5'})
        for e in self.entries:
            if e['scale']=='50m':
                self.assertTrue(e['experiment'].startswith('A'))
            if e['experiment']=='single-ih':
                self.assertEqual(e['source'],'ih/runs.csv')
                self.assertEqual(e['scale'],'100k')
            if e['experiment']=='single-ru':
                self.assertEqual(e['scale'],'500k')
            if e['metric']=='io_per_op':
                self.assertIn(e['experiment'],['A1','A3'])
            if e['metric']=='avg_leaf_density':
                self.assertEqual(e['database'],'postgres')

    def test_ih_selection_is_125_not_128(self):
        p=DOCS/self.data['sources']['ih/selection.json']['path']
        selected=json.loads(p.read_text())
        self.assertEqual(len(selected),125)
        self.assertEqual(len({r['logical_run_id'] for r in selected}),125)
        self.assertEqual(sum(r['selected_stage']=='repeat' for r in selected),3)
        self.assertEqual(sum(len(e['values']) for e in self.entries if e['experiment']=='single-ih' and e['metric']=='throughput'),125)

    def test_cluster_contrasts_agree_with_paper_macros(self):
        macros=dict(re.findall(r'\\newcommand\{\\Num(\w+)\}\{([^}]+)\}',(DOCS/'data/sources/analysis/numbers.tex').read_text()))
        for arm,word in [('A1','AOne'),('A2','ATwo'),('A3','AThree')]:
            for metric,mw in [('throughput','Tput'),('p50_latency_us','PFifty'),('p95_latency_us','PNinetyFive'),('p99_latency_us','PNinetyNine')]+([('io_per_op','IoOp')] if arm!='A2' else []):
                c=self.data['contrasts'][arm][metric]
                self.assertAlmostEqual(float(macros['Nl'+word+mw+'Ratio']),c['ratio'],delta=.00051)
                self.assertAlmostEqual(float(macros['Nl'+word+mw+'CiNinetyFiveLo']),c['ci'][0],delta=.00051)
                self.assertAlmostEqual(float(macros['Nl'+word+mw+'CiNinetyFiveHi']),c['ci'][1],delta=.00051)
                self.assertAlmostEqual(float(macros['Nl'+word+mw+'P']),c['p'],delta=.0000051)

    def test_no_operational_addresses_in_manifests(self):
        for name,source in self.data['sources'].items():
            if not name.endswith('.meta.json'): continue
            m=json.loads((DOCS/source['path']).read_text())
            for field in ['ssh-key','ssh-user','nodes']:
                self.assertEqual(m['flags'][field],'<scrubbed>')
            self.assertEqual(m['effective_cluster']['nodes'],'<scrubbed>')

    def test_attempted_reads_are_not_claimed_all_successful(self):
        readings=[]
        for name,source in self.data['sources'].items():
            if name.startswith('cluster/') and name.endswith('.runs.jsonl') and 'nachlauf_a5_' not in name:
                readings.extend(json.loads(line)['metrics'] for line in (DOCS/source['path']).read_text().splitlines())
        self.assertTrue(all(m['failed']==0 for m in readings))
        self.assertGreater(sum(m['not_found'] for m in readings),0)
        self.assertTrue(all(m['attempted']==m['succeeded']+m['not_found']+m['failed'] for m in readings))
        note=self.data['metrics']['throughput']['note']
        self.assertIn('attempted operations',note)
        self.assertIn('no driver errors',note)
        self.assertIn('some lookups returned no row',note)
        self.assertNotIn('no failed lookups',note)

    def test_io_exclusion_and_method_disclosures(self):
        for arm in ['A1','A2','A3']:
            e=next(e for e in self.entries if e['experiment']==arm)
            records=[json.loads(line)['metrics'] for line in (DOCS/self.data['sources'][e['source']]['path']).read_text().splitlines()]
            median_read=st.median(r['read_iops'] for r in records)
            median_ratio=st.median(r['write_iops']/r['read_iops'] if r['read_iops'] else math.inf for r in records)
            io=self.data['ioContexts'][arm]
            self.assertEqual(io['medianReadIops'],median_read)
            self.assertEqual(io['medianWriteReadPct'],median_ratio*100)
            self.assertEqual(io['excluded'],median_read>1 and median_ratio>.05)
            self.assertEqual(io['excluded'],arm=='A2')
        self.assertIn('unknown amount',self.data['metrics']['write_iops']['note'])
        js=(DOCS/'assets/evidence.js').read_text()
        self.assertIn('connection establishment and full transaction logging',js)
        self.assertIn('pre-run validation can warm caches',js)

    def test_publication_candidate_includes_every_source(self):
        eligible=check_git_eligibility()
        self.assertTrue(all(s['path'] in eligible for s in self.data['sources'].values()))
        with tempfile.TemporaryDirectory() as directory:
            candidate=Path(directory)
            stage_candidate(candidate)
            self.assertEqual(validate_artifact(candidate),49)
            missing=next(s['path'] for s in self.data['sources'].values() if s['path'].endswith('.runs.jsonl'))
            (candidate/missing).unlink()
            with self.assertRaisesRegex(ValueError,'Missing publication file'):
                validate_artifact(candidate)
        # The allowlist must not make private operational manifests eligible.
        result=subprocess.run(['git','check-ignore','--stdin'],input='private.meta.json\nprivate.runs.jsonl\ndocs/data/sources/cluster/unselected.csv.meta.json\n',text=True,capture_output=True,cwd=DOCS.parent,check=False)
        self.assertEqual(result.returncode,0)
        self.assertEqual(len(result.stdout.splitlines()),3)

    def test_active_page_is_independent_of_archival_data(self):
        html=(DOCS/'index.html').read_text()
        js=(DOCS/'assets/evidence.js').read_text()
        self.assertIn('assets/evidence.js',html)
        self.assertNotIn('assets/app.js',html)
        self.assertNotIn('data/data.json',js)
        self.assertNotIn('annotations.json',js)
        self.assertNotIn('chart.js@',html)


if __name__=='__main__':
    unittest.main()
