#!/usr/bin/env python3
"""Browser smoke/interaction checks, with optional batched screenshots.
Requires Python playwright and a Chromium executable (no web dependencies).
python3 scripts/check_evidence_browser.py --screenshots /tmp/uuid-evidence-review
"""
import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import hashlib
import tempfile
from pathlib import Path
import threading
from playwright.sync_api import sync_playwright, expect
from check_evidence_package import stage_candidate

ROOT=Path(__file__).resolve().parent.parent


class QuietHandler(SimpleHTTPRequestHandler):
    def log_message(self,*args): pass


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--screenshots',type=Path)
    parser.add_argument('--chromium',default='/usr/bin/chromium')
    args=parser.parse_args()
    # Serve a Git-eligible packaging rehearsal, not the unrestricted working tree.
    candidate=tempfile.TemporaryDirectory(prefix='uuid-browser-artifact-')
    stage_candidate(Path(candidate.name))
    server=ThreadingHTTPServer(('127.0.0.1',0),partial(QuietHandler,directory=candidate.name))
    threading.Thread(target=server.serve_forever,daemon=True).start()
    base=f'http://127.0.0.1:{server.server_port}/'
    errors=[]
    def check_overflow(page):
        assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth'), 'Page-level horizontal overflow'
    def ready(page):
        page.locator('#content h1').wait_for()
        assert page.locator('#load-state').is_hidden()
    try:
        with sync_playwright() as p:
            browser=p.chromium.launch(executable_path=args.chromium,headless=True,args=['--no-sandbox'])
            context=browser.new_context(viewport={'width':1440,'height':768},reduced_motion='reduce',accept_downloads=True)
            page=context.new_page()
            page.on('pageerror',lambda err:errors.append(str(err)))
            page.goto(base);ready(page)
            manifest=context.request.get(base+'data/evidence.json').json()
            for source in manifest['sources'].values():
                response=context.request.get(base+source['path'])
                assert response.status==200,source['path']
                assert hashlib.sha256(response.body()).hexdigest()==source['sha256'],source['path']
            assert page.locator('.finding').count()==4
            finding_labels=[
                ('PostgreSQL inserts','Throughput: UUIDv4 vs Sequential',['UUIDv4','Seq.']),
                ('PostgreSQL index','Leaf fragmentation: UUIDv4 (absolute)',['UUIDv4','UUIDv7']),
                ('MySQL inserts','Throughput: UUIDv4 vs Sequential',['UUIDv4','Seq.']),
                ('Cassandra reads','Throughput: UUIDv7 vs UUIDv4',['UUIDv7','UUIDv4']),
            ]
            for title,description,keys in finding_labels:
                card=page.locator('.finding').filter(has=page.get_by_role('heading',name=title,exact=True))
                expect(card.locator('p')).to_have_text(description)
                labels=card.locator('.mini-comparison text').all_text_contents()
                assert [labels[0],labels[2]]==keys
            cassandra_finding=page.locator('.finding').filter(has=page.get_by_role('heading',name='Cassandra reads',exact=True))
            read_medians={e['keyType']:e['median'] for e in manifest['entries'] if e['experiment']=='A1' and e['metric']=='throughput'}
            read_ratio=read_medians['UUIDV7']/read_medians['UUIDV4']
            expect(cassandra_finding.locator('strong')).to_have_text(f'+{(read_ratio-1)*100:.0f}%')
            expect(cassandra_finding).to_contain_text('Throughput: UUIDv7 vs UUIDv4')
            expect(cassandra_finding).to_contain_text('read attempts/s')
            assert page.locator('.database-entry').count()==4
            assert page.locator('svg.plot-svg:visible').count()==0
            for card in page.locator('.finding').all():
                assert 'view=explorer' in card.get_attribute('href')
            if args.screenshots:
                args.screenshots.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(args.screenshots/'desktop-summary.png'),full_page=True)
            assert page.evaluate('document.documentElement.scrollHeight <= 768'), 'Summary must fit a desktop screen'
            assert page.locator('.mini-comparison').count()==4
            page.locator('.additional-results > summary').click()
            assert page.locator('svg.plot-svg:visible').count()==5
            assert '3.48' in page.locator('#content').inner_text()
            expect(page.locator('#content')).to_contain_text('Counts read attempts, including no-row responses.')
            assert 'No significant difference is not equivalence' not in page.locator('#load-state').inner_text()
            page.locator('#select-clusterMetric').select_option('table_size_mb')
            assert '18.2%' in page.locator('#content').inner_text()
            assert page.locator(':focus').get_attribute('id')=='select-clusterMetric'
            page.locator('#select-clusterMetric').select_option('throughput')
            page.locator('#select-matrixDb').select_option('mongodb')
            assert page.locator('table.matrix tbody tr').count()==7
            page.locator('#select-matrixDb').select_option('postgres')
            if args.screenshots:
                args.screenshots.mkdir(parents=True,exist_ok=True)
                page.screenshot(path=str(args.screenshots/'desktop-findings.png'),full_page=True)
            check_overflow(page)
            # Direct links, reload, same-view hash navigation, canonical URL repair.
            page.goto(base+'#view=explorer&experiment=A1&metric=throughput');ready(page)
            assert '0.288' in page.locator('.contrast-values').inner_text()
            assert page.locator('#select-db option').count()==1
            assert page.locator('#select-scale option').count()==1
            assert page.locator('#select-mode option').count()==1
            assert '50m' in page.url
            page.locator('#select-metric').select_option('io_per_op')
            assert '3.889' in page.locator('.contrast-values').inner_text()
            page.reload();ready(page)
            assert page.locator('#select-metric').input_value()=='io_per_op'
            page.locator('#select-experiment').select_option('A2')
            assert page.locator('#select-metric option[value="io_per_op"]').count()==0
            assert '0.15079' in page.locator('.contrast').inner_text()
            page.locator('#select-experiment').select_option('A4')
            assert 'n=3' in page.locator('.context').inner_text()
            assert 'Method check' in page.locator('.contrast').inner_text()
            page.locator('#select-experiment').select_option('A5')
            assert '-2.15' in page.locator('.contrast').inner_text()
            page.locator('#select-experiment').select_option('single-ih')
            assert page.locator('#select-scale').input_value()=='100k'
            page.locator('#select-db').select_option('mongodb')
            page.locator('.details-table summary').click()
            assert page.locator('table tbody tr').count()==7
            with page.expect_download() as download:
                page.locator('[data-action="download"]').click()
            csv_path=download.value.path()
            import csv
            with open(csv_path) as handle:
                rows=list(csv.DictReader(handle))
            assert len(rows)==35 and all(r['source']=='ih/runs.csv' for r in rows)
            assert all(r['experiment']=='single-ih' for r in rows)
            page.locator('#select-experiment').select_option('single-ru')
            assert page.locator('#select-scale').input_value()=='500k'
            page.locator('#select-experiment').select_option('single-insert')
            page.locator('#select-mode').select_option('engines')
            assert page.locator('svg.plot-svg').count()==4
            assert page.locator('#select-metric option').count()==1
            assert 'reference=sequential' in page.url
            page.locator('#select-mode').select_option('scales')
            assert page.locator('svg.plot-svg').count()==3
            page.goto(base+'#view=explorer&experiment=A1&metric=throughput');ready(page)
            if args.screenshots: page.screenshot(path=str(args.screenshots/'desktop-explorer.png'),full_page=True)
            assert page.evaluate('document.documentElement.scrollHeight <= 768'), 'Explorer must fit a desktop screen'
            page.locator('.condition-details > summary').click()
            expect(page.locator('.condition-details p').first).to_be_visible()
            page.locator('.condition-details > summary').click()
            page.locator('.contrast details > summary').click()
            expect(page.locator('.contrast details p').first).to_be_visible()
            page.locator('.contrast details > summary').click()
            page.locator('nav [data-view="data"]').click()
            expect(page.locator('#content h1')).to_have_text('Data & methods')
            assert page.locator('.source-list a').count()>5
            if args.screenshots: page.screenshot(path=str(args.screenshots/'desktop-methods.png'),full_page=True)
            assert page.evaluate('document.documentElement.scrollHeight <= 768'), 'Data default view must fit a desktop screen'
            expect(page.locator('[data-action="download"]')).to_be_visible()
            expect(page.locator('.table-actions a[download]')).to_be_visible()
            assert page.locator('.details-table tbody tr').count()==6
            for disclosure in page.locator('.data-disclosures > details').all():
                disclosure.locator(':scope > summary').click()
                assert disclosure.get_attribute('open') is not None
                check_overflow(page)
                disclosure.locator(':scope > summary').click()
            with page.expect_download() as data_download:
                page.locator('[data-action="download"]').click()
            with open(data_download.value.path()) as handle:
                data_rows=list(csv.DictReader(handle))
            assert len(data_rows)==30 and all(r['experiment']=='A1' for r in data_rows)
            # Invalid input is escaped/normalized, and archived deep links migrate.
            page.goto(base+'#view=explorer&experiment=invalid&metric=%3Cscript%3E&db=other&scale=100m');ready(page)
            assert page.locator('#select-experiment').input_value()=='A1'
            assert page.locator('#select-db').input_value()=='cassandra'
            page.goto(base+'#view=explorer&scenario=mixed_insert_heavy&db=mysql&scale=1m&mode=cross-db');ready(page)
            assert page.locator('#select-experiment').input_value()=='single-ih'
            assert page.locator('#select-scale').input_value()=='100k'
            # Same-view navigation must rerender (old router did not).
            page.evaluate("location.hash='view=explorer&experiment=A3&metric=throughput'")
            page.wait_for_function("document.querySelector('#select-experiment')?.value === 'A3'")
            assert '0.341' in page.locator('.contrast-values').inner_text()
            # Back/forward after a filter change restores both DOM and URL.
            page.locator('#select-metric').select_option('p99_latency_us')
            page.go_back()
            page.wait_for_function("document.querySelector('#select-metric')?.value === 'throughput'")
            page.go_forward()
            page.wait_for_function("document.querySelector('#select-metric')?.value === 'p99_latency_us'")
            for width in [1024,390,320]:
                page.set_viewport_size({'width':width,'height':844})
                page.goto(base);ready(page);check_overflow(page)
                if width==1024:
                    assert page.evaluate('document.documentElement.scrollHeight <= 844'), 'Tablet summary should fit one screen'
                if width==390 and args.screenshots: page.screenshot(path=str(args.screenshots/'mobile-findings.png'),full_page=True)
                page.goto(base+'#view=explorer&experiment=A1&metric=throughput');ready(page);check_overflow(page)
                if width==390 and args.screenshots: page.screenshot(path=str(args.screenshots/'mobile-explorer.png'),full_page=True)
                page.goto(base+'#view=data&experiment=single-ih&db=postgres&scale=100k&metric=throughput');ready(page);check_overflow(page)
                if width==390 and args.screenshots: page.screenshot(path=str(args.screenshots/'mobile-methods.png'),full_page=True)
            # Keyboard skip navigation must focus main without changing route or data.
            for view in ['explorer','data']:
                page.goto(base+f'#view={view}&experiment=A3&metric=p99_latency_us');ready(page)
                before=(page.url,page.locator('#content').inner_html())
                page.locator('.skip-link').focus()
                page.keyboard.press('Enter')
                expect(page.locator('#main')).to_be_focused()
                assert (page.url,page.locator('#content').inner_html())==before
                assert page.locator('#select-experiment').input_value()=='A3'
                assert page.locator('#select-metric').input_value()=='p99_latency_us'
            # Failure state is visible and actionable; normal navigation never silently blanks.
            error_page=context.new_page()
            error_page.route('**/data/evidence.json',lambda route:route.fulfill(status=503,body='unavailable'))
            error_page.goto(base)
            error_page.locator('#retry').wait_for()
            assert error_page.locator('#load-state').get_attribute('role')=='alert'
            assert not errors,errors
            browser.close()
        print(json.dumps({'status':'passed','pageErrors':errors,'screenshots':str(args.screenshots) if args.screenshots else None,'checks':'Git-eligible artifact served; all 49 source downloads HTTP 200 + SHA-256; keyboard skip preserves Explorer/Data state; routing, history, filters, experiment isolation, missing endpoint, CSV selection, responsive overflow (1440/1024/390/320), loading failure'},indent=2))
    finally:
        server.shutdown()
        candidate.cleanup()


if __name__=='__main__':main()
