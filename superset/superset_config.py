# Config Superset cho dev local (phien rieng, nhu openmetadata/).
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "bdp-dev-not-secret")

# Tat CSRF de tao dashboard qua REST API bang Bearer token (dev local, khong auth ngoai).
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

# Cho phep query truc tiep tu SQL Lab thanh dataset.
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}
