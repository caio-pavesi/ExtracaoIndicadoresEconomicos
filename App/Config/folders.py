from os import path
from pathlib import Path

USER = Path(path.expanduser('~'))
ONEDRIVE = Path(f"C:/Users/{ USER.parts[-1] }/XP Investimentos")