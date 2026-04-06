from .overview_routes import bp as overview_bp
from .settings_routes import bp as settings_bp
from .accounts_routes import bp as accounts_bp
from .scraper_routes import bp as scraper_bp
from .groups_routes import bp as groups_bp
from .logs_routes import bp as logs_bp
from .permissions_routes import bp as permissions_bp
from .assignments_routes import bp as assignments_bp
from .campaigns_routes import bp as campaigns_bp
from .automation_routes import bp as automation_bp
from .recovery_routes import bp as recovery_bp
from .orchestrator_routes import bp as orchestrator_bp
from .diagnosa_routes import bp as diagnosa_bp


def register_blueprints(app):
    for bp in [
        overview_bp,
        settings_bp,
        accounts_bp,
        scraper_bp,
        groups_bp,
        logs_bp,
        permissions_bp,
        assignments_bp,
        campaigns_bp,
        automation_bp,
        recovery_bp,
        orchestrator_bp,
        diagnosa_bp,
    ]:
        app.register_blueprint(bp)
