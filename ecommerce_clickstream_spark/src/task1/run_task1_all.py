import os
import subprocess
import sys


MODULES = [
    "task1.clean_events_spark",
    "task1.clean_supporting_tables_spark",
    "task1.validate_events_output",
    "task1.validate_relationships_spark",
    "task1.validate_business_rules",
]


def main():
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    for index, module_name in enumerate(MODULES, start=1):
        print(f"[{index}/{len(MODULES)}] Running {module_name}")
        subprocess.run(
            [sys.executable, "-m", module_name],
            check=True,
            env=env,
        )


if __name__ == "__main__":
    main()
