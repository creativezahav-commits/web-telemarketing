from __future__ import annotations

from flask import Blueprint

from services.orchestrator_service import (
    execute_recovery_safe,
    get_orchestrator_status,
    run_full_cycle,
    scan_recovery_items,
)
from utils.api import body, fail, ok

bp = Blueprint('orchestrator_v2', __name__, url_prefix='/api/v2')


@bp.get('/orchestrator/status')
def orchestrator_status_v2():
    return ok(get_orchestrator_status())


@bp.post('/orchestrator/run')
def orchestrator_run_v2():
    payload = body()
    result = run_full_cycle(trigger=payload.get('trigger') or 'manual_api')
    if not result.get('ok') and result.get('busy'):
        return ok(result, 'Orchestrator sedang berjalan')
    if not result.get('ok'):
        return fail(result.get('error') or 'Orchestrator gagal', 500, details=result)
    return ok(result, 'Orchestrator selesai dijalankan')


@bp.post('/orchestrator/scan-recovery')
def orchestrator_scan_recovery_v2():
    return ok(scan_recovery_items(), 'Recovery scan selesai')


@bp.post('/orchestrator/execute-recovery')
def orchestrator_execute_recovery_v2():
    return ok(execute_recovery_safe(), 'Recovery execute selesai')
