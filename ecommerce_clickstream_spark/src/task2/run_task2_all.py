# ============================================================
# TASK 2 — RUN ALL PIPELINE
# ============================================================
# Ý tưởng:
# - File này dùng để chạy toàn bộ Task 2.
# - Task 2 được chia thành 3 phần riêng:
#
#   1. active_users_spark.py
#      -> tính most active users
#
#   2. top_products_spark.py
#      -> tính top products
#
#   3. peak_activity_spark.py
#      -> tính peak activity time
#
# - Chạy file này sẽ chạy lần lượt cả 3 phần.
# ============================================================

from task2.active_users_spark import main as run_active_users
from task2.top_products_spark import main as run_top_products
from task2.peak_activity_spark import main as run_peak_activity
from task2.validate_task2 import main as validate_task2


def main():
    print("========== RUNNING FULL TASK 2 PIPELINE ==========")

    print("\n[1/3] Running Most Active Users...")
    run_active_users()

    print("\n[2/3] Running Top Products...")
    run_top_products()

    print("\n[3/3] Running Peak Activity Time...")
    run_peak_activity()

    print("\n[4/4] Validating Task 2 outputs...")
    validate_task2()

    print("\n========== FULL TASK 2 PIPELINE COMPLETED ==========")


if __name__ == "__main__":
    main()
