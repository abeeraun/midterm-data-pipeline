خط بيانات هجين لمعالجة بيانات الطلبات

مشروع نصفي لمقرر البيانات الضخمة العملي. يستخدم المشروع Python Batch للملفات الصغيرة، وApache Spark للملفات الكبيرة، وMongoDB للتخزين، مع تنفيذ نمط ELT: تحميل البيانات الخام أولًا ثم التنظيف والتصنيف.

1. المتطلبات

•
Python 3.10 أو أحدث.

•
Java 17 أو أحدث لتشغيل PySpark.

•
MongoDB يعمل على mongodb://127.0.0.1:27017 أو URI بديل.

•
مساحة كافية على القرص للملف المصدر وملفات Spark المؤقتة.

2. التثبيت والإعداد

على Windows:

Plain Text


python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt



على Linux/macOS:

Bash


python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt



تُحفظ الإعدادات في config/settings.py أو متغيرات البيئة. الإعدادات الأساسية المستخدمة في التشغيل:

Plain Text


SMALL_FILE_THRESHOLD_MB = 200
BATCH_SIZE = 1000
MONGO_BATCH_SIZE = 500
SPARK_MASTER = local[2]
SPARK_SHUFFLE_PARTITIONS = 8
COLLECTION_RAW = orders_raw
COLLECTION_VALIDATED = orders_validated
COLLECTION_QUARANTINE = orders_quarantine



3. التشغيل

إنشاء عينة قابلة للإعداد

Plain Text


python -m src.create_small_sample --input data\orders_huge_mixed_quality.csv --output data\orders_sample_500.csv --rows 500 --seed 42



ينتج الأمر 500 سجل بيانات و501 سطرًا عند احتساب Header. يمكن تغيير قيمة --rows دون تعديل الكود.

التشغيل العادي عبر Router

Plain Text


python -m src.main --input data\orders_sample.csv
python -m src.main --input data\orders_huge_mixed_quality.csv



يفحص Router حجم الملف ويستخدم python_batch إذا كان الحجم أقل من أو يساوي 200MB، ويستخدم pyspark إذا تجاوز الحجم ذلك الحد. يطبع Router حجم الملف والمحرك وسبب الاختيار.

الاختبارات

Plain Text


python -m pytest -q



النتيجة المسجلة:

Plain Text


25 passed in 0.13s



4. مراحل خط البيانات

1.
قراءة المسار والحجم وإنشاء id_run.

2.
اختيار المحرك تلقائيًا.

3.
تحميل السجلات إلى orders_raw قبل التنظيف.

4.
تطبيق التحويلات وقواعد الجودة.

5.
تصنيف السجلات إلى valid أو corrected أو quarantine.

6.
استخدام Upsert للسجلات المقبولة في orders_validated، وكتابة السجلات المعزولة في orders_quarantine.

7.
حفظ المقاييس في ملفات JSON.

تحتفظ طبقة Raw بالتشغيلات التاريخية المتعددة، ولذلك يجب استخدام id_run أو file_source عند فحص تشغيل محدد.

5. قواعد التنظيف والتصنيف

توجد قواعد التنظيف في src/quality_rules.py، ويُحفظ أثر التصحيح في corrections:

القاعدة
الرمز
تحويل الأرقام العربية والفارسية
ARABIC_DIGITS_CONVERTED
توحيد العملة
CURRENCY_UNIFIED
تطبيع الأرقام وفواصل الآلاف
تطبيع رقمي
تطبيع الهاتف
PHONE_FORMAT_NORMALIZED
تصحيح الرموز المتكررة في البريد
EMAIL_REPEATED_SYMBOLS
توحيد التاريخ إلى ISO
DATE_FORMAT_NORMALIZED
إزالة المسافات وتوحيد المرادفات
TEXT_FIELD_TRIMMED_NORMALIZED
إعادة حساب الإجمالي
ORDER_TOTAL_RECALCULATED




أسباب العزل المستخدمة في التنفيذ:

Plain Text


MISSING_ORDER_ID
MISSING_CUSTOMER_ID
INVALID_IMPOSSIBLE_DATE
CORRUPTED_ITEMS_JSON
EMPTY_ITEMS
UNKNOWN_PRICE
AMBIGUOUS_NEGATIVE_VALUE
DUPLICATE_ORDER_ID
MULTIPLE_CONFLICTING_ERRORS
EMAIL_UNFIXABLE



يحتوي مستند العزل على error_codes وerror_details وrecord_raw.

6. Collections وIdempotency

Plain Text


orders_raw          السجلات الخام وبيانات المصدر والتشغيل
orders_validated    السجلات السليمة والمصححة القابلة للاستخدام
orders_quarantine   السجلات التي لم يمكن تصحيحها بأمان



يستخدم المشروع order_id كمفتاح عمل ثابت. توجد في orders_validated Unique Index باسم uniq_order_id، وتتم الكتابة باستخدام Upsert.

إعادة تشغيل نفس العينة أثبتت:

Plain Text


count_inserted = 0
count_updated = 0
count_unchanged = 11136



كما أُجري اختبار تحديث لسجل موجود:

Plain Text


order_id = طلب-100002
القيمة قبل التحديث: city = حجة
القيمة بعد التحديث: city = عدن
count_inserted = 0
count_updated = 1
count_unchanged = 11135
target_matches = 1



7. نتائج التشغيل الكبير

المرجع الرسمي للمقاييس هو reports/timing_run.json، وسجل التنفيذ هو timing_run.log:

Plain Text


file_name = data\\orders_huge_mixed_quality.csv
file_size_mb = 12650.32
used_engine = pyspark
read_rows = 30000000
loaded_raw = 30000000
count_valid = 23513283
count_corrected = 2866644
count_quarantine = 3620073
seconds_elapsed = 27569.953
throughput_rows_per_sec = 1088.14



فحص الاتساق:

Plain Text


30000000 = 23513283 + 2866644 + 3620073
raw_equals_valid_plus_corrected_plus_quarantine = true
EXIT_CODE = 0



8. مقارنة المحركات على العينة

تم تشغيل العينة نفسها عبر Router الطبيعي وعبر PySpark الاختباري باستخدام حد --threshold-mb 1:

المقياس
Python Batch
PySpark
السجلات
12,008
12,008
الزمن بالثواني
9.7663
60.3969
السجلات/الثانية
1,229.53
198.82
Partitions
غير منطبق
2
valid
8,830
9,293
corrected
2,306
1,193
quarantine
872
1,522




يظهر الفرق في زمن العينة بسبب كلفة تهيئة Spark وJVM، بينما صُمم Spark للتعامل مع الملفات الكبيرة عبر المعالجة الموزعة وDataFrame API.

9. المقاييس والملفات

تُحفظ المقاييس في:

Plain Text


reports/results.json
reports/timing_run.json



وتشمل id_run واسم الملف وحجمه والمحرك وعدد السجلات وأعداد التصنيف والزمن والإنتاجية وbatch_size أو partitions وعدادات inserted/updated/unchanged وفحص الاتساق.

ملف reports/timing_run.json هو المرجع الخاص بتشغيل الملف الكبير. يحتوي reports/results.json على سجل التشغيل الكبير بعد دمجه مع سجلات تشغيلات العينة.

10. بنية المشروع

Plain Text


midterm-data-pipeline/
├── README.md
├── requirements.txt
├── .env.example
├── config/
│   └── settings.py
├── data/
│   ├── orders_sample.csv
│   └── orders_huge_mixed_quality.csv
├── src/
│   ├── main.py
│   ├── file_router.py
│   ├── create_small_sample.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── quality_rules.py
│   ├── elt_pipeline.py
│   ├── incremental_loader.py
│   ├── mongo_setup.py
│   └── metrics.py
├── tests/
│   └── test_classification.py
├── reports/
│   ├── results.json
│   ├── timing_run.json
│   ├── results_final.md
│   └── screenshots/
└── docs/
    └── architecture.md



11. تنظيف الموارد

تُغلق SparkSession واتصالات MongoDB داخل finally في src/main.py:

Python


if spark_session is not None:
    spark_session.stop()
client.close()



12. ملفات التسليم

Plain Text


README.md
requirements.txt
config/settings.py
src/
tests/
reports/results.json
reports/timing_run.json
reports/results_final.md
reports/screenshots/
timing_run.log
presentation_samples.json



