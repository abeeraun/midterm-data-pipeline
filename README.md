Hybrid Order Data Pipeline

مشروع عملي لمقرر البيانات الضخمة لبناء Hybrid Data Pipeline لمعالجة بيانات طلبات غير نظيفة باستخدام Python Batch وPySpark وMongoDB.

يطبق المشروع نمط ELT؛ إذ يتم تحميل السجلات الخام أولًا إلى MongoDB، ثم تطبيق Data Cleaning وData Quality Rules، وبعد ذلك تصنيف السجلات إلى Valid وCorrected وQuarantine.

1. المتطلبات

يحتاج المشروع إلى Python 3.10 أو أحدث، وJava 17 أو أحدث لتشغيل PySpark، وMongoDB يعمل على localhost:27017 أو على MongoDB URI آخر يتم تحديده في الإعدادات.

يحتاج تشغيل الملف الكبير إلى مساحة كافية للملف الأصلي وملفات Spark المؤقتة وبيانات MongoDB.

2. تثبيت المشروع

على Windows:

Plain Text


python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt



على Linux أو macOS:

Bash


python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt



يجب تشغيل MongoDB قبل بدء Pipeline.

3. إعدادات المشروع

توجد الإعدادات الأساسية في config/settings.py، ويمكن استخدام Environment Variables للقيم التي تختلف بين الأجهزة. من أهم الإعدادات:

Plain Text


SMALL_FILE_THRESHOLD_MB = 200
BATCH_SIZE = 1000
MONGO_BATCH_SIZE = 500
SPARK_MASTER = local[2]
SPARK_SHUFFLE_PARTITIONS = 200
COLLECTION_RAW = orders_raw
COLLECTION_VALIDATED = orders_validated
COLLECTION_QUARANTINE = orders_quarantine



يختار File Router Python Batch عندما يكون حجم الملف أقل من أو يساوي 200 MB، ويختار PySpark عندما يتجاوز الملف هذا الحد.

4. تشغيل المشروع

نقطة التشغيل الرئيسية هي src.main. يقوم البرنامج بقراءة مسار الملف، وحساب حجمه، وإنشاء run_id، ثم اختيار Processing Engine المناسب.

لتشغيل ملف صغير باستخدام Python Batch:

Plain Text


python -m src.main --input data\orders_sample.csv --threshold-mb 100



لتشغيل ملف كبير باستخدام PySpark:

Plain Text


python -m src.main --input data\orders_huge_mixed_quality.csv --threshold-mb 0



تستخدم قيمة threshold تساوي 0 في اختبار PySpark لإجبار Router على اختيار PySpark حتى مع ملف صغير.

5. إنشاء Sample للاختبار

يحتوي المشروع على Script مستقل لإنشاء Sample من الملف الكبير دون استخدام Excel أو تعديل البيانات يدويًا.

Plain Text


python -m src.create_small_sample --input data\orders_huge_mixed_quality.csv --output data\orders_sample_500.csv --rows 500 --seed 42



قيمة rows قابلة للتغيير. ينتج الأمر ملفًا يحتوي على Header وعدد السجلات المطلوب.

6. مراحل Pipeline

تبدأ العملية بقراءة مسار الملف وحجمه وإنشاء run_id. بعد ذلك يختار File Router المحرك المناسب.

يتم تحميل جميع السجلات إلى orders_raw قبل تطبيق أي Cleaning أو Quality Filtering. بعد ذلك تطبق قواعد التحويل والتحقق، ثم يصنف كل سجل إلى Valid أو Corrected أو Quarantine.

تتم كتابة السجلات Valid وCorrected إلى orders_validated باستخدام Idempotent Upsert، بينما تتم كتابة السجلات غير القابلة للتصحيح إلى orders_quarantine مع سبب العزل والسجل الخام.

في النهاية تحفظ Metrics الخاصة بكل Run في reports/results.json.

7. Python Batch Loader

يستخدم Python Batch Loader قراءة Streaming من خلال csv module، ويعالج السجلات في Batches قابلة للضبط بدل تحميل الملف كاملًا إلى الذاكرة.

لكل Batch يتم تسجيل عدد السجلات والزمن وThroughput. يتم استخدام insert_many لكتابة Raw وعمليات Bulk Upsert لكتابة Validated.

8. PySpark Loader

يستخدم PySpark Loader SparkSession وDataFrame API وSchema ثابتة بدل inferSchema. تتم قراءة الحقول الحساسة كقيم String في Raw للمحافظة على القيم الأصلية قبل التنظيف.

يستخدم PySpark MongoDB Connector للكتابة إلى MongoDB، ويسجل عدد Input Partitions والزمن وThroughput. تم ضبط SPARK_SHUFFLE_PARTITIONS على 200 لتقليل الضغط على الذاكرة عند معالجة الملف الكبير.

9. قواعد Data Quality

يطبق المشروع أكثر من ثماني قواعد تنظيف وتصحيح، من أهمها تحويل Arabic وPersian Digits، وتوحيد Currency، ومعالجة Thousand Separators، وتحويل الأسعار المكتوبة بالكلمات، وتطبيع Phone Number، وتصحيح Repeated Email Symbols، وتوحيد Date Format، وإزالة المسافات وتوحيد المرادفات، وإعادة حساب Order Total عند صلاحية مكوناته.

لا يتم التصحيح إلا عندما تكون قاعدة التحويل واضحة. أما القيم التي لا يمكن تصحيحها بأمان فتنتقل إلى Quarantine.

كل سجل Corrected يحتفظ بـAudit Trail داخل corrections، ويشمل Field وOriginal Value وCorrected Value وRule Code.

10. Quarantine

تحتوي orders_quarantine على السجلات التي لا يمكن تصحيحها بأمان. يتضمن كل مستند Run ID وSource File وSource Row Number وError Codes وError Details وRaw Record.

من أمثلة أسباب العزل Missing Order ID وMissing Customer ID وInvalid or Impossible Date وCorrupted Items JSON وEmpty Items وUnknown Price وAmbiguous Negative Value وDuplicate Order ID وMultiple Conflicting Errors.

11. MongoDB Collections

orders_raw تحتوي على السجلات الأصلية كما وصلت، مع بيانات المصدر والتشغيل.

orders_validated تحتوي على السجلات Valid وCorrected القابلة للاستخدام، مع Quality Status وCorrections وRecord Hash.

orders_quarantine تحتوي على السجلات التي لم يمكن تصحيحها، مع أسباب العزل والسجل الخام.

يحتوي orders_validated على Unique Index باسم uniq_order_id على order_id، ويستخدم order_id كـStable Business Key في Upsert. كما تم تفعيل Schema Validation للحقول الأساسية.

12. Idempotency وUpsert

تم اختبار إعادة تشغيل نفس Sample في Python Batch وPySpark. في التشغيل الثاني لم يزد عدد المستندات في orders_validated، ولم تظهر Business Records مكررة.

في اختبار Python Batch الثاني كانت النتائج count_inserted تساوي 0 وcount_updated تساوي 0 وcount_unchanged تساوي 466.

وفي اختبار PySpark الثاني كانت النتائج count_inserted تساوي 0 وcount_updated تساوي 0 وcount_unchanged تساوي 466.

تم أيضًا تعديل سجل موجود ثم إعادة تشغيل Pipeline. في اختبار PySpark ظهرت النتائج count_inserted تساوي 0 وcount_updated تساوي 1 وcount_unchanged تساوي 465، مع بقاء عدد المستندات في orders_validated مساويًا 466.

13. نتائج التشغيل الكبير

تم تشغيل الملف الكبير باستخدام PySpark بنجاح:

Plain Text


data/orders_huge_mixed_quality.csv



الملف يحتوي على 30,000,000 سجل وحجمه 12,650.32 MB.

كانت النتائج النهائية كما يلي:

Metric
Result
Engine
PySpark
Rows read
30,000,000
Loaded Raw
30,000,000
Valid
22,343,466
Corrected
5,697,571
Quarantine
1,958,963
Input Partitions
99
Processing Time
19,913.98 seconds
Throughput
1,506.48 records per second
Inserted into Validated
28,041,037




تم التحقق من Consistency Check:

Plain Text


30,000,000 = 22,343,466 + 5,697,571 + 1,958,963



كما أن عدد السجلات التي دخلت orders_validated يساوي Valid زائد Corrected:

Plain Text


28,041,037 = 22,343,466 + 5,697,571



14. النتائج والمقاييس

تحفظ Metrics في reports/results.json. تتضمن النتائج Run ID واسم الملف وحجمه وProcessing Engine وعدد السجلات المقروءة والسجلات المحملة إلى Raw وأعداد Valid وCorrected وQuarantine والزمن وThroughput وBatch Size أو Partitions وعدادات Inserted وUpdated وUnchanged وConsistency Check.

يحتوي reports/results.md على ملخص النتائج النهائية، بينما يحتوي docs/architecture.md على وصف Architecture الخاص بالمشروع.

15. الاختبارات

لتشغيل الاختبارات:

Plain Text


python -m pytest -q



آخر نتيجة مسجلة للاختبارات هي:

Plain Text


25 passed



تغطي الاختبارات قواعد Cleaning وClassification الأساسية. كما تم تنفيذ اختبارات يدوية لـClassification Parity وIdempotency وDuplicate Prevention وUpdate في Python Batch وPySpark.

16. إغلاق الموارد

يستخدم المشروع try/finally لإغلاق SparkSession واتصال MongoDB حتى عند حدوث خطأ أثناء التشغيل.

17. بنية المشروع

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
│   ├── test_cleaning_rules.py
│   └── test_classification.py
├── reports/
│   ├── results.md
│   ├── results.json
│   └── screenshots/
└── docs/
    └── architecture.md

