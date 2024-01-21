import enum
import os

import dotenv


class EnvType(enum.Enum):
    local = "local"


def load_env():
    env_type = os.environ.get("ENV_TYPE", EnvType.local.value)
    dotenv.load_dotenv(f"env/{env_type}.env")
