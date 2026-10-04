import argparse
import sys
import uuid

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, RESULTS_JSON_PATH
from src.file_router import decide_engine
from src.mongo_setup import ensure_collections
from src.metrics import RunMetrics, append_run_to_results_file
from src.batch_loader import run_batch_load
from src.incremental_loader import run_incremental_load


def parse_args():
    parser = argparse.ArgumentParser(description="خط البيانات الهجين - نقطة التشغيل الرئيسية")
    parser.add_argument("--input", required=True, help="مسار ملف CSV المراد معالجته")
    parser.add_argument("--threshold-mb", type=float, default=None,
                         help="تجاوز الحد الفاصل بين Python Batch وPySpark لهذا التشغيل فقط")
    parser.add_argument("--batch-size", type=int, default=None, help="حجم الدفعة لمحرك Python Batch")
    parser.add_argument(
        "--stage", choices=["normal", "initial", "delta"], default="normal",
        help="normal = تشغيل عادي حسب Router. initial/delta = مسار B التزايدي (يفرض Python Batch دائمًا)",
    )
    return parser.parse_args()


def run_ingest(input_path, threshold_mb=None, batch_size=None, stage="normal"):
    id_run = str(uuid.uuid4())

    print(f"===== ??? ????? ???? | id_run = {id_run} =====")

    decision = decide_engine(input_path, threshold_mb=threshold_mb)

    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    metrics = RunMetrics(
        id_run=id_run,
        file_name=decision["file_path"],
        file_size_mb=decision["file_size_mb"],
        used_engine=decision["engine"] if stage == "normal" else f"incremental_{stage}",
    )

    spark_session = None

    try:
        ensure_collections(db)

        if stage in ("initial", "delta"):
            run_incremental_load(
                input_path,
                db,
                id_run,
                metrics,
                stage_label=stage,
                batch_size=batch_size or 1000,
            )

        elif decision["engine"] == "python_batch":
            run_batch_load(
                input_path,
                db,
                id_run,
                metrics,
                batch_size=batch_size,
            )

        else:
            from src.spark_loader import build_spark_session, run_spark_load

            spark_session = build_spark_session()
            run_spark_load(
                input_path,
                spark_session,
                id_run,
                metrics,
            )

        metrics.finalize()
        result_dict = metrics.to_dict()

        ok, expected = metrics.consistency_check()

        if not ok:
            print(
                f"[run_ingest] Warning: consistency check failed! "
                f"loaded_raw={metrics.loaded_raw}, "
                f"valid+corrected+quarantine={expected}"
            )
        else:
            print(
                "[run_ingest] Consistency check passed: "
                "raw = valid + corrected + quarantine [OK]"
            )

        append_run_to_results_file(result_dict)

        print("===== ???? ??????? =====")

        for key in (
            "used_engine",
            "read_rows",
            "loaded_raw",
            "count_valid",
            "count_corrected",
            "count_quarantine",
            "seconds_elapsed",
            "throughput_rows_per_sec",
            "count_inserted",
            "count_updated",
            "count_unchanged",
        ):
            print(f"  {key}: {result_dict[key]}")

        return result_dict

    finally:
        if spark_session is not None:
            spark_session.stop()
            print("[run_ingest] ?? ????? SparkSession.")

        client.close()
        print("[run_ingest] ?? ????? ????? MongoDB.")


def main():
    args = parse_args()

    run_ingest(
        input_path=args.input,
        threshold_mb=args.threshold_mb,
        batch_size=args.batch_size,
        stage=args.stage,
    )


if __name__ == "__main__":
    sys.exit(main() or 0)
