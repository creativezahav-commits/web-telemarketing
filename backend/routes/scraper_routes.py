from __future__ import annotations

from flask import Blueprint, request

from services.scraper_service import control_scrape_job, import_scrape_results, preview_scrape_keywords, start_scrape_job
from utils.api import body, fail, ok, pagination_args, parse_bool, parse_int
from utils.storage_db import get_scrape_job, get_scrape_jobs, get_scrape_keyword_runs, get_scrape_results

bp = Blueprint('scraper_v2', __name__, url_prefix='/api/v2')


@bp.get('/scraper/summary')
def scraper_summary_v2():
    jobs = get_scrape_jobs(limit=100)
    running = [j for j in jobs if j.get('status') in {'queued', 'running', 'paused'}]
    return ok({
        'active_jobs': len(running),
        'pending_keywords': sum(max(0, int((j.get('total_keywords') or 0) - (j.get('processed_keywords') or 0))) for j in running),
        'results_today': sum(int(j.get('total_saved') or 0) for j in jobs[:20]),
        'average_job_duration': None,
        'success_rate': round(sum(1 for j in jobs if j.get('status') == 'done') / len(jobs) * 100, 2) if jobs else 0,
    })


@bp.post('/scraper/jobs/preview')
def scraper_preview_v2():
    payload = body()
    base_keyword = payload.get('base_keyword') or payload.get('keywords_text') or ''
    options = payload.get('options') or payload
    preview = preview_scrape_keywords(base_keyword, options)
    return ok(preview)


@bp.post('/scraper/jobs')
def scraper_start_v2():
    payload = body()
    phone = payload.get('account_id') or payload.get('phone') or 'auto'
    raw_keywords = payload.get('base_keyword') or payload.get('keywords_text') or ''
    if not raw_keywords.strip():
        return fail('base_keyword wajib diisi')
    options = payload.get('options') or payload
    result = start_scrape_job(phone, raw_keywords, options)
    return ok(result, 'Scrape job berhasil dibuat', status_code=201)


@bp.get('/scraper/jobs')
def scraper_jobs_v2():
    jobs = get_scrape_jobs(limit=parse_int(request.args.get('limit'), 50, minimum=1, maximum=200))
    return ok({'items': jobs})


@bp.get('/scraper/jobs/<int:job_id>')
def scraper_job_detail_v2(job_id: int):
    job = get_scrape_job(job_id)
    if not job:
        return fail('Job tidak ditemukan', 404)
    return ok({'job': job, 'stats': {
        'keywords': get_scrape_keyword_runs(job_id)[:5],
        'result_count': len(get_scrape_results(job_id)),
    }})


@bp.get('/scraper/jobs/<int:job_id>/keywords')
def scraper_job_keywords_v2(job_id: int):
    page, page_size = pagination_args()
    rows = get_scrape_keyword_runs(job_id)
    status = request.args.get('status')
    if status:
        rows = [r for r in rows if r.get('status') == status]
    total = len(rows)
    start = (page - 1) * page_size
    return ok({'items': rows[start:start + page_size]}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.post('/scraper/jobs/<int:job_id>/<action>')
def scraper_job_control_v2(job_id: int, action: str):
    if action not in {'pause', 'resume', 'stop'}:
        return fail('Aksi tidak didukung', 404)
    return ok(control_scrape_job(job_id, action))


@bp.post('/scraper/jobs/<int:job_id>/retry-failed')
def scraper_job_retry_failed_v2(job_id: int):
    return ok(control_scrape_job(job_id, 'retry_failed'))


@bp.get('/scraper/results')
def scraper_results_v2():
    job_id = parse_int(request.args.get('job_id'), 0)
    if not job_id:
        return fail('job_id wajib diisi')
    page, page_size = pagination_args()
    rows = get_scrape_results(
        job_id,
        only_recommended=parse_bool(request.args.get('only_recommended')),
        only_new=parse_bool(request.args.get('only_new')),
        include_imported=not parse_bool(request.args.get('only_not_imported')),
    )
    total = len(rows)
    start = (page - 1) * page_size
    return ok({'items': rows[start:start + page_size]}, meta={'page': page, 'page_size': page_size, 'total': total})


@bp.get('/scraper/results/summary')
def scraper_results_summary_v2():
    job_id = parse_int(request.args.get('job_id'), 0)
    rows = get_scrape_results(job_id) if job_id else []
    return ok({
        'total_results': len(rows),
        'high_quality_count': sum(1 for r in rows if int(r.get('relevance_score') or 0) >= 70),
        'ready_import_count': sum(1 for r in rows if not r.get('already_in_db') and not r.get('imported')),
        'imported_count': sum(1 for r in rows if r.get('imported')),
        'duplicate_count': sum(1 for r in rows if r.get('already_in_db')),
        'recommended_count': sum(1 for r in rows if r.get('recommended')),
    })


@bp.get('/scraper/results/<int:result_id>')
def scraper_result_detail_v2(result_id: int):
    # lightweight lookup through all recent jobs
    jobs = get_scrape_jobs(limit=50)
    target = None
    for job in jobs:
        for row in get_scrape_results(job['id']):
            if row['id'] == result_id:
                target = row
                break
        if target:
            break
    if not target:
        return fail('Result tidak ditemukan', 404)
    return ok({'overview': target, 'scoring': {
        'quality_score': target.get('relevance_score', 0),
        'quality_tier': 'high' if int(target.get('relevance_score') or 0) >= 70 else 'medium' if int(target.get('relevance_score') or 0) >= 45 else 'low',
        'reason_flags': target.get('catatan', ''),
    }})


@bp.post('/scraper/results/import')
def scraper_result_import_v2():
    payload = body()
    job_id = parse_int(payload.get('job_id'), 0)
    if not job_id:
        return fail('job_id wajib diisi')
    result_ids = payload.get('result_ids') or []
    mode = payload.get('mode') or ('selected' if result_ids else 'recommended')
    result = import_scrape_results(job_id, result_ids, mode)
    return ok(result, 'Import selesai')
