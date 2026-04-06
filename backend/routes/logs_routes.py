from __future__ import annotations

from datetime import datetime
from flask import Blueprint, request

from utils.api import ok, pagination_args
from utils.database import get_conn
from utils.storage_db import get_audit_logs

bp = Blueprint('logs_v2', __name__, url_prefix='/api/v2')


@bp.get('/logs/summary')
def logs_summary_v2():
    conn = get_conn()
    today = datetime.now().strftime('%Y-%m-%d') + '%'
    delivery = conn.execute(
        """
        SELECT
          SUM(CASE WHEN status IN ('gagal','send_failed','join_failed') THEN 1 ELSE 0 END) AS errors_today,
          SUM(CASE WHEN status IN ('berhasil','send_success','join','join_success') THEN 1 ELSE 0 END) AS success_today,
          COUNT(*) AS total_today
        FROM riwayat WHERE waktu LIKE %s
        """,
        (today,),
    ).fetchone()
    audit = conn.execute(
        """
        SELECT
          SUM(CASE WHEN level='warning' THEN 1 ELSE 0 END) AS warnings_today,
          SUM(CASE WHEN module='recovery' THEN 1 ELSE 0 END) AS recoveries_today,
          SUM(CASE WHEN level='error' THEN 1 ELSE 0 END) AS system_alerts
        FROM audit_log WHERE created_at LIKE %s
        """,
        (today,),
    ).fetchone()
    conn.close()
    return ok({
        'errors_today': int((delivery['errors_today'] if delivery else 0) or 0),
        'success_today': int((delivery['success_today'] if delivery else 0) or 0),
        'total_today': int((delivery['total_today'] if delivery else 0) or 0),
        'warnings_today': int((audit['warnings_today'] if audit else 0) or 0),
        'recoveries_today': int((audit['recoveries_today'] if audit else 0) or 0),
        'system_alerts': int((audit['system_alerts'] if audit else 0) or 0),
    })


@bp.get('/logs')
def logs_list_v2():
    page, page_size = pagination_args()
    audit_items, audit_total = get_audit_logs(
        level=request.args.get('level'),
        module=request.args.get('module'),
        entity_type=request.args.get('entity_type'),
        action=request.args.get('action'),
        page=page,
        page_size=page_size,
    )
    items = [
        {
            'id': f"audit-{row['id']}",
            'timestamp': row.get('created_at'),
            'level': row.get('level') or 'info',
            'module': row.get('module') or 'system',
            'entity': row.get('entity_type') or 'unknown',
            'action': row.get('action') or 'event',
            'result': row.get('result') or 'logged',
            'message': row.get('message') or '',
            'raw': row,
        }
        for row in audit_items
    ]
    return ok({'items': items}, meta={'page': page, 'page_size': page_size, 'total': audit_total})
