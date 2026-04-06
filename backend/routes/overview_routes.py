from __future__ import annotations

from flask import Blueprint, request

from services.overview_service import (
    get_active_processes,
    get_attention_items,
    get_overview_health,
    get_overview_summary,
    get_pipeline_flow,
    get_trends,
    get_group_state_dashboard,
    get_automation_diagnostics,
)
from utils.api import ok

bp = Blueprint('overview_v2', __name__, url_prefix='/api/v2')


@bp.get('/overview/summary')
def overview_summary():
    return ok(get_overview_summary())


@bp.get('/overview/health')
def overview_health():
    return ok(get_overview_health())


@bp.get('/overview/processes')
def overview_processes():
    return ok({"items": get_active_processes()})


@bp.get('/overview/attention')
def overview_attention():
    return ok(get_attention_items())


@bp.get('/overview/trends')
def overview_trends():
    return ok(get_trends(request.args.get('range', '7d')))


@bp.get('/overview/flow')
def overview_flow():
    return ok(get_pipeline_flow())


@bp.get('/overview/group-states')
def overview_group_states():
    search = request.args.get('search', '')
    focus_state = request.args.get('focus_state', '')
    include_archived = str(request.args.get('include_archived', '0')).lower() in {'1','true','yes','on'}
    try:
        limit_per_state = int(request.args.get('limit_per_state', 20) or 20)
    except Exception:
        limit_per_state = 20
    return ok(get_group_state_dashboard(search=search, focus_state=focus_state, limit_per_state=limit_per_state, include_archived=include_archived))


@bp.get('/overview/automation-diagnostics')
def overview_automation_diagnostics():
    return ok(get_automation_diagnostics())
