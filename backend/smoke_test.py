from __future__ import annotations

import json
from typing import Any

import app as app_module

app = app_module.app
client = app.test_client()

CHECKS: list[tuple[str, str, dict[str, Any] | None, tuple[int, ...]]] = [
    ('GET', '/api/health', None, (200,)),
    ('GET', '/api/settings', None, (200,)),
    ('GET', '/api/automation/status', None, (200,)),
    ('GET', '/api/flow', None, (200,)),
    ('GET', '/api/v2/overview/summary', None, (200,)),
    ('GET', '/api/v2/overview/flow', None, (200,)),
    ('GET', '/api/v2/overview/group-states', None, (200,)),
    ('GET', '/api/v2/orchestrator/status', None, (200,)),
    ('GET', '/api/v2/automation-rules/summary', None, (200,)),
    ('GET', '/api/v2/automation-rules/meta', None, (200,)),
    ('GET', '/api/v2/automation-rules/meta?stage=delivery', None, (200,)),
    ('POST', '/api/v2/automation-rules/validate', {'stage': 'delivery', 'condition_json': {'pending_count_gte': 1, 'online_accounts_gte': 1, 'active_draft_required': True}, 'action_json': {'limit': 10, 'retry_delay_minutes': 10, 'require_active_draft': True, 'require_online_sender': True, 'blocked_terms': ['blocked']}, 'scope_json': {'target_status_in': ['queued'], 'campaign_status_in': ['queued']}}, (200,)),
    ('GET', '/api/v2/automation-rules/overview', None, (200,)),
    ('GET', '/api/v2/settings', None, (200,)),
    ('GET', '/api/v2/settings/grouped', None, (200,)),
    ('GET', '/api/v2/permissions/summary', None, (200,)),
    ('GET', '/api/v2/assignments/summary', None, (200,)),
    ('GET', '/api/v2/assignments/criteria', None, (200,)),
    ('GET', '/api/v2/campaigns/summary', None, (200,)),
    ('GET', '/api/v2/recovery/summary', None, (200,)),
    ('GET', '/api/v2/logs/summary', None, (200,)),
    ('POST', '/api/v2/recovery/scan', {}, (200,)),
    ('POST', '/api/v2/orchestrator/run', {'trigger': 'smoke_test'}, (200,)),
    ('POST', '/api/v2/permissions/recheck-expired', {}, (200,)),
    ('POST', '/api/v2/assignments/run-auto', {'limit': 10}, (200,)),
    ('POST', '/api/v2/broadcast-queue/pause', {}, (200,)),
    ('POST', '/api/v2/broadcast-queue/resume', {}, (200,)),
    ('POST', '/api/v2/settings/restore-defaults', {'scope': 'automation'}, (200,)),
    ('POST', '/api/akun/hapus', {'phone': '+62000000000'}, (200,)),
    ('GET', '/api/v2/settings/grouped-not-found', None, (404,)),
]


def run() -> int:
    failed = 0
    for method, path, payload, expected in CHECKS:
        response = getattr(client, method.lower())(path, json=payload)
        ok = response.status_code in expected
        data = response.get_data(as_text=True)
        marker = 'PASS' if ok else 'FAIL'
        print(f'[{marker}] {method} {path} -> {response.status_code}')
        if not ok:
            failed += 1
            print(data[:500])
    return failed


if __name__ == '__main__':
    total_failed = run()
    raise SystemExit(1 if total_failed else 0)
